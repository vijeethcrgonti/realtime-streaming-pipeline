"""
streaming_stack.py  —  cdk/lib/
Provisions MSK (Kafka), Kinesis Data Streams, Kinesis Firehose,
EMR Serverless application, and all associated IAM roles.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_emrserverless as emr,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_msk as msk,
    aws_s3 as s3,
)
from constructs import Construct


class StreamingStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        vpc: ec2.Vpc,
        processed_bucket: s3.Bucket,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── Kinesis Data Streams ───────────────────────────────────────────────

        self.clickstream_stream = kinesis.Stream(
            self, "ClickstreamStream",
            stream_name=f"clickstream-events-{stage}",
            shard_count=4,
            retention_period=cdk.Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        self.orders_stream = kinesis.Stream(
            self, "OrdersStream",
            stream_name=f"orders-created-{stage}",
            shard_count=2,
            retention_period=cdk.Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        # ── Kinesis Firehose → S3 (archive raw events) ────────────────────────

        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        processed_bucket.grant_read_write(firehose_role)

        firehose.CfnDeliveryStream(
            self, "ClickstreamFirehose",
            delivery_stream_name=f"clickstream-firehose-{stage}",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.clickstream_stream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=processed_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="raw/clickstream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                error_output_prefix="raw/errors/clickstream/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=300,
                    size_in_m_bs=128,
                ),
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=True,
                    output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                        serializer=firehose.CfnDeliveryStream.SerializerProperty(
                            parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                                compression="SNAPPY",
                            )
                        )
                    ),
                    input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                        deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                            open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty()
                        )
                    ),
                    schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                        database_name="etl_catalog",
                        region=self.region,
                        role_arn=firehose_role.role_arn,
                        table_name="clickstream_events",
                    ),
                ),
            ),
        )

        # ── Amazon MSK (Kafka) ─────────────────────────────────────────────────

        msk_sg = ec2.SecurityGroup(
            self, "MSKSecurityGroup",
            vpc=vpc,
            description="MSK Kafka cluster",
            allow_all_outbound=True,
        )
        msk_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block),
            ec2.Port.tcp(9092),
            "Kafka plaintext from VPC",
        )
        msk_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block),
            ec2.Port.tcp(9094),
            "Kafka TLS from VPC",
        )

        private_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnet_ids

        self.msk_cluster = msk.CfnCluster(
            self, "MSKCluster",
            cluster_name=f"streaming-kafka-{stage}",
            kafka_version="3.5.1",
            number_of_broker_nodes=3,
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.t3.small",
                client_subnets=private_subnets[:3],
                security_groups=[msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=100,
                    )
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS_PLAINTEXT",
                    in_cluster=True,
                )
            ),
        )

        # ── EMR Serverless Application ─────────────────────────────────────────

        emr_role = iam.Role(
            self, "EMRRole",
            role_name=f"EMRServerlessRole-{stage}",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
        )
        processed_bucket.grant_read_write(emr_role)
        self.clickstream_stream.grant_read(emr_role)

        self.emr_app = emr.CfnApplication(
            self, "SparkApp",
            name=f"streaming-spark-{stage}",
            type="SPARK",
            release_label="emr-6.15.0",
            auto_stop_configuration=emr.CfnApplication.AutoStopConfigurationProperty(
                enabled=True,
                idle_timeout_minutes=15,
            ),
            maximum_capacity=emr.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="40 vCPU",
                memory="160 GB",
            ),
        )

        cdk.CfnOutput(self, "ClickstreamStreamName", value=self.clickstream_stream.stream_name)
        cdk.CfnOutput(self, "OrdersStreamName", value=self.orders_stream.stream_name)
        cdk.CfnOutput(self, "EMRAppId", value=self.emr_app.attr_application_id)

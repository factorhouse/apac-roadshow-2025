import argparse
import logging

from confluent_kafka.error import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_topics(
    admin_client: AdminClient,
    topic_names: list,
    num_partitions: int = 1,
    replication_factor: int = 1,
):
    """
    Create multiple Kafka topics and return a report of their creation status.

    Returns a dict with topic_name -> status:
        - "created"          : successfully created
        - "exists"           : topic already exists
        - "failed"           : failed to create
    """
    new_topics = [
        NewTopic(name, num_partitions, replication_factor) for name in topic_names
    ]
    result_dict = admin_client.create_topics(new_topics)

    for topic, future in result_dict.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.warning(f"Topic '{topic}' already exists.")
            else:
                logger.error(f"Failed to create topic '{topic}': {e}")


def delete_topics(admin_client: AdminClient, topic_names: list):
    """
    Delete multiple Kafka topics and return a report of their deletion status.

    Returns a dict with topic_name -> status:
        - "deleted"          : successfully deleted
        - "not_found"        : topic does not exist
        - "failed"           : failed to delete
    """
    report = {}
    result_dict = admin_client.delete_topics(topic_names)

    for topic, future in result_dict.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' deleted")
            report[topic] = "deleted"
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logger.warning(f"Topic '{topic}' does not exist.")
                report[topic] = "not_found"
            else:
                logger.error(f"Failed to delete topic '{topic}': {e}")
                report[topic] = "failed"

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Kafka Admin Client Config")
    parser.add_argument(
        "--action",
        choices=["create", "delete"],
        default="create",
        help="Action to perform on topics: create (default) or delete",
    )

    # --- Connection Arguments ---
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        required=True,
        help="Comma-separated Kafka bootstrap servers",
    )
    parser.add_argument("--username", "-u", required=True, help="SASL username")
    parser.add_argument("--password", "-p", required=True, help="SASL password")

    args = parser.parse_args()

    topics = [
        "websocket_fanout",
        "processing_fanout",
        "ecommerce_events",
        "ecommerce_processing_fanout",
        "product-updates",
        "recommendations",
        "inventory_updates",
        "inventory-events",
        "shopping-cart-events",
        "basket-patterns",
        "order-events",
        "product-recommendations",
    ]

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": args.username,
        "sasl.password": args.password,
    }
    admin_client = AdminClient(conf)
    if args.action == "create":
        create_topics(admin_client, topics)
    else:
        delete_topics(admin_client, topics)

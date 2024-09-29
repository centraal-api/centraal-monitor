""" Monitor Azure queues for poison messages and handle them. """

from azure.storage.queue import QueueServiceClient
import logging


class QueueMonitor:
    def __init__(self, connection_string):
        self.queue_service = QueueServiceClient.from_connection_string(
            conn_str=connection_string
        )

    def monitor_poison_queue(self, poison_queue_name):
        poison_queue = self.queue_service.get_queue_client(poison_queue_name)
        properties = poison_queue.get_queue_properties()
        message_count = properties.approximate_message_count

        if message_count > 0:
            # Logic to create an alert
            logging.warning(
                f"Poison queue '{poison_queue_name}' has {message_count} messages."
            )
            messages = poison_queue.receive_messages(messages_per_page=message_count)
            for msg in messages:
                # Process each poison message
                self.handle_poison_message(msg, poison_queue_name)

    def handle_poison_message(self, message, poison_queue_name):
        # Logic to forward the message to another system or log details
        logging.info(
            f"Processing poison message from '{poison_queue_name}': {message.content}"
        )
        # Delete the message after processing
        poison_queue = self.queue_service.get_queue_client(poison_queue_name)
        poison_queue.delete_message(message)

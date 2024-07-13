const amqp = require('amqplib');

const RABBITMQ_URL = 'amqp://localhost';
const MAIN_QUEUE = 'main_queue';
const DLQ_QUEUE = 'dead_letter_queue';
const EXCHANGE = 'main_exchange';
const ROUTING_KEY = 'main_key';

async function setup() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        // Declare the dead letter queue
        await channel.assertQueue(DLQ_QUEUE, { durable: true });

        // Declare the main queue with dead letter exchange parameters
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': DLQ_QUEUE,
            },
        });

        // Declare the exchange
        await channel.assertExchange(EXCHANGE, 'direct', { durable: true });

        // Bind the main queue to the exchange
        await channel.bindQueue(MAIN_QUEUE, EXCHANGE, ROUTING_KEY);

        // Function to simulate message processing and rejecting (sending to DLQ)
        const processMessage = (msg) => {
            if (msg !== null) {
                const content = msg.content.toString();
                console.log(`Received message from ${MAIN_QUEUE}: ${content}`);

                // Simulate processing failure for messages containing the word "fail"
                if (content.includes('fail')) {
                    console.log(`Rejecting message ${content}, sending to DLQ`);
                    channel.nack(msg, false, false);
                } else {
                    console.log(`Acknowledging message ${content}`);
                    channel.ack(msg);
                }
            }
        };

        // Consume messages from the main queue
        await channel.consume(MAIN_QUEUE, processMessage, { noAck: false });

        // Function to print the number of messages in the DLQ
        const printDLQMessageCount = async () => {
            const { messageCount } = await channel.checkQueue(DLQ_QUEUE);
            console.log(`Number of messages in DLQ (${DLQ_QUEUE}): ${messageCount}`);
        };

        // Periodically check and print the number of messages in the DLQ
        setInterval(printDLQMessageCount, 5000); // Check every 5 seconds

        console.log('Waiting for messages. To exit press CTRL+C');
    } catch (error) {
        console.error('Error setting up RabbitMQ:', error);
    }
}

setup();

async function consumeDLQ() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(DLQ_QUEUE, { durable: true });

        setInterval(async () => {
            const { messageCount } = await channel.checkQueue(DLQ_QUEUE);
            
            if (messageCount > 0) {
                console.log(`*********** Number of messages in DLQ (${DLQ_QUEUE}): ${messageCount}`);
                // Consume messages from DLQ
                channel.consume(DLQ_QUEUE, (msg) => {
                    if (msg !== null) {
                        console.log(`*********** Process a received message from DLQ (${DLQ_QUEUE}): ${msg.content.toString()}`);
                        // Process the message here as needed
                        channel.ack(msg); // Acknowledge message after processing
                    }
                });
                console.log(`*********** Number of messages in DLQ (${DLQ_QUEUE}): ${messageCount}`);
            }
        }, 5000); // Check every 5 seconds
    } catch (error) {
        console.error('Error consuming DLQ:', error);
    }
}

consumeDLQ();

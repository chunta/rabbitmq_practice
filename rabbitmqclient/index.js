const amqp = require('amqplib');

const RABBITMQ_URL = 'amqp://localhost';
const EXCHANGE = 'main_exchange';
const ROUTING_KEY = 'main_key';

async function createChannel() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertExchange(EXCHANGE, 'direct', { durable: true });
        return { connection, channel };
    } catch (error) {
        console.error('Error creating RabbitMQ channel:', error);
        throw error;
    }
}

async function sendMessage(channel, message) {
    try {
        await channel.publish(EXCHANGE, ROUTING_KEY, Buffer.from(message));
        console.log(`Message sent to ${EXCHANGE} with routing key ${ROUTING_KEY}: ${message}`);
    } catch (error) {
        console.error('Error sending message to RabbitMQ:', error);
    }
}

let messageCount = 0;

async function startSendingMessages() {
    const { connection, channel } = await createChannel();

    const messages = [
        'Hello from the client!',
        'This message will fail',
        'Another good message',
        'fail message',
        'This should pass',
    ];

    let index = 0;

    setInterval(() => {
        messageCount++;
        const message = `${messages[index]} ${messageCount}`;
        sendMessage(channel, message);
        index = (index + 1) % messages.length;
    }, 2000);

    // Clean up gracefully on process termination
    process.on('SIGINT', async () => {
        console.log('Closing RabbitMQ connection...');
        await channel.close();
        await connection.close();
        process.exit(0);
    });
}

startSendingMessages();

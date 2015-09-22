package main

import (
	"github.com/streadway/amqp"
)

const RABBITMQ_IP = "192.168.1.121"
const RABBITMQ_USER = "guest"
const RABBITMQ_PWD = "guest"


 

type QueueMgr struct {
     conn *amqp.Connection
	 channel  *amqp.Channel
	 queueType string
	
}



func (q *QueueMgr) publish(msg string) error {
	 if err := q.channel.Publish("", q.queueType, false, false, amqp.Publishing{
        ContentType: "text/plain",
		Body: []byte(msg),
     }); 
	err != nil {
         return err
     }
	return nil
}

func (q *QueueMgr) consume(msg string) (<-chan amqp.Delivery,error) {
	return q.channel.Consume(
		   q.queueType, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}



func  QueueMgFactory(queueType string) (*QueueMgr,error) {
	url := "amqp://" + RABBITMQ_PWD +":" + RABBITMQ_PWD + "@" + RABBITMQ_IP + ":5672"
 	conn, err := amqp.Dial(url)
    if err != nil {
		defer conn.Close()
        return nil,err
    }
    //build channel in the connection
    channel, err := conn.Channel()
        if err != nil {
		defer channel.Close()
        return nil,err
    }
    //queue declare
    if _, err := channel.QueueDeclare(queueType, false, true, false, false, nil); err != nil {
        return  nil,err
    }
	 return &QueueMgr{conn,channel,queueType},nil
}

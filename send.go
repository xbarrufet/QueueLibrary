package main

import (
	"fmt"
	"log"
)


 func main() {
	
	qmgr,err := QueueMgFactory("TEST_POC")
	fmt.Println("queue created")
	if err!=nil {
	  panic(fmt.Sprintf("Error connecting to queue %s %s",err,qmgr.queueType))
	}
	
	for i:=0;i<10;i++ {
		err= qmgr.publish(fmt.Sprintf("hola:%s\n", i))
		if err!=nil {
	  		panic(fmt.Sprintf("Error sending msg %s",err))
		}		
		fmt.Printf("Missage sent %s\n",i)
	} 

	
	msgs,err2 := qmgr.consume("TEST_POC")
	if err2!=nil {
	  fmt.Println(fmt.Sprintf("Error sending msg %s",err))
	}
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			d.Ack(false)
			log.Printf("Done missatge 1:%s\n", d.Body)
		}
	}()
	
	go func() {
		for d := range msgs {
			d.Ack(false)
			log.Printf("Done missatge 2:%s", d.Body)
		}
	}()
	
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C") 
	<-forever
}
	
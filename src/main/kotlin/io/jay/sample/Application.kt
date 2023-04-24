package io.jay.sample

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.*
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.function.Function

@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}

@RestController
@RequestMapping("/sample")
class Controller(val producer: EventProducer) {

    @PostMapping
    fun send() {
        producer.produce()
    }
}

@Configuration
class ProducerConfig {
    @Bean
    fun producer(): Sinks.Many<String> {
        return Sinks.many().multicast().onBackpressureBuffer()
    }

    @Bean
    fun producerSupplier(sinks: Sinks.Many<String>) = Supplier<Flux<String>> {
        sinks.asFlux()
    }
}

@Component
class EventProducer(val sinks: Sinks.Many<String>) {

    fun produce() {
        val id = UUID.randomUUID().toString()
        println("EventProducer::produce $id")
        sinks.tryEmitNext(id)
    }
}

@Configuration
class ConsumerConfig {
}

@Component
class EventConsumer {

    @Bean
    fun consumer() = Consumer<Message<String>> {
        val ack = it.headers.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment::class.java)
        println("EventConsumer::consume ${MyLogger.log(it)}")
        if (ack != null) {
            println("Acknowledgement provided for ${it.payload}")
            ack.acknowledge()
        }
    }
}

@Configuration
class ProcessorConfig {
}

@Component
class EventProcessor {
    @Bean
    fun processorFunction() = Function<Flux<Message<String>>, Flux<String>> {
        it.map { message ->
            println("EventProcessor::processorFunction ${MyLogger.log(message)}")
            message.payload.uppercase()
        }
    }
}

class MyLogger {
    companion object {
        fun log(message: Message<String>) : String {
            return "From: [${message.headers.get(KafkaHeaders.RECEIVED_TOPIC, String::class.java)}], Payload: [${message.payload}]"
        }
    }
}


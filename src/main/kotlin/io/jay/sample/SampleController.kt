package io.jay.sample

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

@RestController
@RequestMapping("/sample")
class SampleController(val producer: EventProducer) {

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
        println("EventConsumer::consume ${it.payload}")
        if (ack != null) {
            println("Acknowledgement provided for ${it.payload}")
            ack.acknowledge()
        }
    }
}


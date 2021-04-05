package com.unisinos.kafkaproducer.controller;

import com.unisinos.kafkaproducer.model.PaymentModel;
import com.unisinos.kafkaproducer.model.response.PaymentResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class TestController {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "pagamento";

    @PostMapping("/publish")
    public PaymentModel Message(@RequestBody PaymentModel paymentModel) {
        try {
            var paymentResponse = executeTransaction(paymentModel);
            kafkaTemplate.send(TOPIC, paymentResponse);
            return paymentModel;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private PaymentResponse executeTransaction(PaymentModel paymentModel) {
        return PaymentResponse.builder()
                .orderId(paymentModel.getOrderId())
                .status("CONFIRMADO")
                .build();
    }
}

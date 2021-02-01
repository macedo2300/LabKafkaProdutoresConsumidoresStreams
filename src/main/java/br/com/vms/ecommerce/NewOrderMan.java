package br.com.vms.ecommerce;

import br.com.vms.ecommerce.model.Email;
import br.com.vms.ecommerce.model.Order;
import br.com.vms.ecommerce.service.KafkaProducerService;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMan {

    private static final String ECOMMERCE_SEND_EMAIL  = "ECOMMERCE_SEND_EMAIL";
    private static final String ECOMMERCE_NEW_ORDER   = "ECOMMERCE_NEW_ORDER";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderKafkaProducerService = new KafkaProducerService<Order>()){
            try (var emailKafkaProducerService = new KafkaProducerService<Email>()){
                for(int i = 0; i < 10; i++){
                    sendEcommerceNewOrder(orderKafkaProducerService);
                    sendEcommerceSendEmail(emailKafkaProducerService);
                }
            }
        }
    }

    private static void sendEcommerceSendEmail(KafkaProducerService kafkaProducerService) throws InterruptedException, ExecutionException {
        var key = UUID.randomUUID().toString();
        var email = "Thank you for your order89!! We are  processing your order!";
        kafkaProducerService.send(ECOMMERCE_SEND_EMAIL,key,email);
    }

    private static void sendEcommerceNewOrder(KafkaProducerService kafkaProducerService) throws InterruptedException, ExecutionException {
        var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        var amount = new BigDecimal(Math.random() * 5000 + 1);

        var order = new Order(userId, orderId, amount);
        kafkaProducerService.send(ECOMMERCE_NEW_ORDER,userId,order);
    }




}

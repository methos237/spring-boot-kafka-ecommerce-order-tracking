package com.jamespolk.ordertracking.order.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import com.jamespolk.ordertracking.order.domain.Order;
import com.jamespolk.ordertracking.order.domain.OrderLine;
import com.jamespolk.ordertracking.order.domain.OrderNotFoundException;
import com.jamespolk.ordertracking.order.domain.OrderService;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    MockMvcTester mvc;

    @MockitoBean
    OrderService orderService;

    @Test
    void createsOrderWithLocationHeader() {
        Order order = Order.place("customer-1", List.of(new OrderLine("SKU-1", 1, new BigDecimal("10.00"))));
        given(orderService.placeOrder(anyString(), any())).willReturn(order);

        var result = mvc.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                        "{\"customerId\":\"customer-1\",\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1,\"unitPrice\":10.00}]}")
                .exchange();

        result.assertThat().hasStatus(HttpStatus.CREATED);
        result.assertThat().headers().hasValue("Location", "/api/orders/" + order.getId());
        result.assertThat().bodyJson().extractingPath("$.totalAmount").isEqualTo(10.00);
    }

    @Test
    void listsEveryInvalidField() {
        var result = mvc.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"customerId\":\" \",\"items\":[{\"sku\":\"\",\"quantity\":0,\"unitPrice\":0}]}")
                .exchange();

        result.assertThat().hasStatus(HttpStatus.BAD_REQUEST);
        result.assertThat().bodyJson().extractingPath("$.errors").asArray().hasSize(4);
    }

    @Test
    void mapsUnknownOrderToNotFoundProblem() {
        UUID id = UUID.randomUUID();
        given(orderService.getOrder(id)).willThrow(new OrderNotFoundException(id));

        var result = mvc.get().uri("/api/orders/{id}", id).exchange();

        result.assertThat().hasStatus(HttpStatus.NOT_FOUND);
        result.assertThat().bodyJson().extractingPath("$.detail").isEqualTo("No order with id " + id);
    }
}

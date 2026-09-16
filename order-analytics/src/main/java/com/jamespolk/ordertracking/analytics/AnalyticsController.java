package com.jamespolk.ordertracking.analytics;

import com.jamespolk.ordertracking.analytics.AnalyticsQueries.CustomerRevenue;
import com.jamespolk.ordertracking.analytics.AnalyticsQueries.MinuteCount;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/analytics")
class AnalyticsController {

    private final AnalyticsQueries queries;

    AnalyticsController(AnalyticsQueries queries) {
        this.queries = queries;
    }

    @GetMapping("/orders-per-minute")
    List<MinuteCount> ordersPerMinute(@RequestParam(defaultValue = "10") int last) {
        return queries.ordersPerMinute(last);
    }

    @GetMapping("/customers/{customerId}/revenue")
    CustomerRevenue revenue(@PathVariable String customerId) {
        return queries.revenue(customerId);
    }
}

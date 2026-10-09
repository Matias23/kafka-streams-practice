package com.mricotta.kafkalab.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mricotta.kafkalab.dto.CustomerTotalResponse;
import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.exception.CustomerTotalNotFoundException;
import com.mricotta.kafkalab.mapper.CustomerTotalMapper;
import com.mricotta.kafkalab.repository.CustomerTotalRepository;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CustomerTotalServiceTest {

    @Mock
    private CustomerTotalRepository customerTotalRepository;

    @Spy
    private CustomerTotalMapper customerTotalMapper = new CustomerTotalMapper();

    @InjectMocks
    private CustomerTotalService customerTotalService;

    @Test
    void storesTheTotal() {
        var total = new CustomerTotal("c1", 2, new BigDecimal("300"));

        customerTotalService.store(total);

        verify(customerTotalRepository).save(total);
    }

    @Test
    void returnsTheStoredTotal() {
        when(customerTotalRepository.findByCustomerId("c1"))
                .thenReturn(Optional.of(new CustomerTotal("c1", 2, new BigDecimal("300"))));

        var response = customerTotalService.getTotal("c1");

        assertThat(response).isEqualTo(new CustomerTotalResponse("c1", 2, new BigDecimal("300")));
    }

    @Test
    void failsWhenTheCustomerHasNoTotal() {
        when(customerTotalRepository.findByCustomerId("unknown")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> customerTotalService.getTotal("unknown"))
                .isInstanceOf(CustomerTotalNotFoundException.class)
                .hasMessageContaining("unknown");
    }
}

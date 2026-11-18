package com.pinchez.wallet.account.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.wallet.account.entity.Account;
import com.pinchez.wallet.account.service.AccountService;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

@WebMvcTest(AccountController.class)
class AccountControllerTest {

    @Autowired
    MockMvcTester mvc;

    @MockitoBean
    AccountService accountService;

    @Test
    void invalidBodyReturnsValidationProblemWithFieldErrors() {
        var result = mvc.post()
                .uri("/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"phoneNumber": "0711", "fullName": ""}
                        """);

        assertThat(result).hasStatus(HttpStatus.BAD_REQUEST).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(result).bodyJson().extractingPath("$.type").isEqualTo("urn:pesaflow:problem:validation-failed");
        assertThat(result)
                .bodyJson()
                .extractingPath("$.errors[*].field")
                .asArray()
                .containsExactlyInAnyOrder("phoneNumber", "fullName");
    }

    @Test
    void unknownAccountReturnsNotFoundProblem() {
        var id = UUID.randomUUID();
        given(accountService.get(id)).willThrow(new ResourceNotFoundException(Account.class, id));

        var result = mvc.get().uri("/accounts/{id}", id);

        assertThat(result).hasStatus(HttpStatus.NOT_FOUND);
        assertThat(result).bodyJson().extractingPath("$.type").isEqualTo("urn:pesaflow:problem:resource-not-found");
        assertThat(result).bodyJson().extractingPath("$.identifier").isEqualTo(id.toString());
        assertThat(result).bodyJson().extractingPath("$.instance").isEqualTo("/accounts/" + id);
    }

    @Test
    void malformedIdReturnsValidationProblem() {
        var result = mvc.get().uri("/accounts/not-a-uuid");

        assertThat(result).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(result).bodyJson().extractingPath("$.errors[0].field").isEqualTo("id");
        assertThat(result).bodyJson().extractingPath("$.errors[0].message").isEqualTo("must be a valid UUID");
    }

    @Test
    void unparseableBodyReturnsMalformedRequestProblem() {
        var result = mvc.post()
                .uri("/accounts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{not json");

        assertThat(result).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(result).bodyJson().extractingPath("$.type").isEqualTo("urn:pesaflow:problem:malformed-request");
    }
}

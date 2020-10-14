package com.libraryinventory.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.libraryinventory.libraryeventsconsumer.entity.LibraryEvent;
import com.libraryinventory.libraryeventsconsumer.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(
            ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        log.info("Inside processLibraryEvent");

        LibraryEvent libraryEvent = objectMapper.readValue(
                consumerRecord.value(), LibraryEvent.class);

        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        log.info("libraryEvent : {}", libraryEvent);
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is missing!");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository
                .findById(libraryEvent.getLibraryEventId());

        if (!libraryEventOptional.isPresent()) {
            throw new IllegalArgumentException("Not a valid library Event!");
        }
        log.info("Validation is successful for the library Event : {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the library Event {}", libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer, String> record) {
        log.info("Inside handleRecovery");
        Integer key = record.key();
        String message = record.value();
        ListenableFuture<SendResult<Integer, String>> listenableFuture =
                kafkaTemplate.sendDefault(key, message);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, message, result);
            }
        });
    }

    private void handleFailure(Throwable ex) {
        log.error("Error Sending the message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in onFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent Successfully for the key {} and the value is {} and partition {}",
                key, value, result.getRecordMetadata().partition());
    }
}
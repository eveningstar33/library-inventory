package com.libraryinventory.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.libraryinventory.libraryeventsconsumer.entity.Book;
import com.libraryinventory.libraryeventsconsumer.entity.LibraryEvent;
import com.libraryinventory.libraryeventsconsumer.entity.LibraryEventType;
import com.libraryinventory.libraryeventsconsumer.jpa.LibraryEventsRepository;
import com.libraryinventory.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                                  "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIT {

    private static final Integer LIBRARY_EVENT_ID = 123;
    private static final Integer LIBRARY_EVENT_ID_000 = 000;
    private static final String JSON_ADD = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Shogun\",\"bookAuthor\":\"James Clavell\"}}";
    private static final String JSON_UPDATE_NOT_NULL_ID = "{\"libraryEventId\":" + LIBRARY_EVENT_ID + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Shogun\",\"bookAuthor\":\"James Clavell\"}}";
    private static final String JSON_UPDATE_NULL_ID = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Shogun\",\"bookAuthor\":\"James Clavell\"}}";
    private static final String JSON_UPDATE_000_ID = "{\"libraryEventId\":" + LIBRARY_EVENT_ID_000 + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Shogun\",\"bookAuthor\":\"James Clavell\"}}";
    private static final String UPDATED_BOOK_NAME = "Shogun 2";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

//    @AfterEach
//    void tearDown() {
//        libraryEventsRepository.deleteAll();
//    }

    @Test
    public void publishNewLibraryEvent()
            throws JsonProcessingException, ExecutionException, InterruptedException {

        kafkaTemplate.sendDefault(JSON_ADD).get();  // sendDefault() is an asynchronous message and using get() I make it synchronous

        blockThisThread();
        verifyMethodCalls(1);

        List<LibraryEvent> libraryEvents = libraryEventsRepository.findAll();
        assert libraryEvents.size() == 1;
        libraryEvents.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    public void publishUpdateLibraryEvent()
            throws JsonProcessingException, ExecutionException, InterruptedException {

        LibraryEvent libraryEvent = objectMapper
                .readValue(JSON_ADD, LibraryEvent.class);

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(123)
                .bookName(UPDATED_BOOK_NAME)
                .bookAuthor("James Clavell")
                .build();

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(
                libraryEvent.getLibraryEventId(), updatedJson).get();

        blockThisThread();
        verifyMethodCalls(1);

        LibraryEvent persistedLibraryEvent = libraryEventsRepository
                .findById(libraryEvent.getLibraryEventId()).get();

        assertEquals(UPDATED_BOOK_NAME, persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    public void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        kafkaTemplate.sendDefault(LIBRARY_EVENT_ID, JSON_UPDATE_NOT_NULL_ID).get();

        blockThisThread();
        verifyMethodCalls(1);
        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository
                .findById(LIBRARY_EVENT_ID);

        assertFalse(libraryEventOptional.isPresent());
    }

    @Test
    public void publishModifyLibraryEvent_Null_LibraryEventId()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        kafkaTemplate.sendDefault(null, JSON_UPDATE_NULL_ID).get();
        blockThisThread();
        verifyMethodCalls(1);
    }

    @Test
    public void publishModifyLibraryEvent_000_LibraryEventId()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        kafkaTemplate.sendDefault(null, JSON_UPDATE_000_ID).get();
        blockThisThread();
        verifyMethodCalls(4);

        verify(libraryEventsServiceSpy, times(1))
                .handleRecovery(isA(ConsumerRecord.class));
    }

    private void blockThisThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
    }

    private void verifyMethodCalls(int noOfTimes) throws JsonProcessingException {
        verify(libraryEventsConsumerSpy, times(noOfTimes))
                .onMessage(isA(ConsumerRecord.class));

        verify(libraryEventsServiceSpy, times(noOfTimes))
                .processLibraryEvent(isA(ConsumerRecord.class));
    }
}
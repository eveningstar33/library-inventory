package com.libraryinventory.libraryeventsconsumer.jpa;

import com.libraryinventory.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventsRepository
        extends JpaRepository<LibraryEvent, Integer> {
}

package com.service.bookinghotels.services.impl;
import com.service.bookinghotels.entities.Booking;
import com.service.bookinghotels.entities.Room;
import com.service.bookinghotels.entities.UnavailableDate;
import com.service.bookinghotels.exceptions.EntityNotFoundException;
import com.service.bookinghotels.exceptions.RoomIsBusyException;
import com.service.bookinghotels.mappers.booking.BookingMapper;
import com.service.bookinghotels.repositories.BookingRepository;
import com.service.bookinghotels.repositories.RoomRepository;
import com.service.bookinghotels.services.BookingService;
import com.service.bookinghotels.services.UnavailableDateService;
import com.service.bookinghotels.utils.BeanUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.starter.outbox.Outbox;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class BookingServiceImpl implements BookingService {
    private final BookingRepository bookingRepository;
    private final RoomRepository roomRepository;
    private final UnavailableDateService unavailableDateService;
    private final Outbox outbox;
    private final BookingMapper bookingMapper;

    @Value("${app.kafka.bookingRoomTopic}")
    private String bookingRoomTopic;

    @Transactional
    @Override
    public Booking createBooking(Booking booking, UserDetails user) {
        LocalDate now = LocalDate.now();
        if (booking.getCheckInDate().isBefore(now) || booking.getCheckOutDate().isBefore(now)) {
            log.error("Check-in or check-out date is before current date: {}", booking);
            throw new IllegalArgumentException("Check-in or check-out date is before current date");
        }
        log.info("Call method createBooking to create booking: {}", booking);
        setNewBusyDatesForRoom(booking);
        Booking newBooking = bookingRepository.save(booking);

        outbox.send(bookingRoomTopic, newBooking.getId().toString(),
                bookingMapper.bookingToBookingRoomEvent(newBooking, user));
        return newBooking;
    }

    @Transactional
    @Override
    public Booking updateBooking(Long id, Booking booking) {
        log.info("Call method updateBooking to update booking: {}", booking);
        Booking existedBooking = getBookingById(id);
        deletePreviousBusyDatesForRoom(existedBooking);
        setNewBusyDatesForRoom(booking);
        BeanUtils.copyNonNullProperties(booking, existedBooking);
        return bookingRepository.save(existedBooking);
    }

    @Transactional
    @Override
    public void deleteBooking(Long id) {
        log.info("Call method deleteBooking to delete booking with id: {}", id);
        Booking booking = getBookingById(id);
        deletePreviousBusyDatesForRoom(booking);
        bookingRepository.deleteById(id);
    }

    @Transactional(readOnly = true)
    @Override
    public Booking getBookingById(Long id) {
        log.info("Call method getBookingById to find booking with id: {}", id);
        return bookingRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Booking is not found"));
    }

    @Transactional(readOnly = true)
    @Override
    public List<Booking> getAllBookings() {
        log.info("Call method getAllBookings");
        return bookingRepository.findAll();
    }

    @Transactional
    private void setNewBusyDatesForRoom(Booking booking) {
        Room room = booking.getRoom();
        Set<LocalDate> datesToLive = new HashSet<>();
        for (LocalDate i = booking.getCheckInDate();
             !i.isEqual(booking.getCheckOutDate().plusDays(1));
             i = i.plusDays(1)) {
            datesToLive.add(i);
        }
        if (room.getBusyDates().stream().map(UnavailableDate::getDate).anyMatch(datesToLive::contains)) {
            log.error("Room is busy on dates: {}", datesToLive);
            throw new RoomIsBusyException(MessageFormat
                    .format("Room is busy on some or all of pointed dates: {0}. Total busy dates: {1}",
                            datesToLive, room.getBusyDates().stream().map(UnavailableDate::getDate).collect(Collectors.toSet())));
        }
        unavailableDateService.addBusyDates(room.getId(), datesToLive);
        roomRepository.save(room);
    }

    @Transactional
    private void deletePreviousBusyDatesForRoom(Booking booking) {
        Room room = booking.getRoom();
        Set<LocalDate> datesToLiveBeforeUpdating = new HashSet<>();
        for (LocalDate i = booking.getCheckInDate();
             !i.isEqual(booking.getCheckOutDate().plusDays(1));
             i = i.plusDays(1)) {
            datesToLiveBeforeUpdating.add(i);
        }
        unavailableDateService.deleteBusyDates(room.getId(), datesToLiveBeforeUpdating);
        roomRepository.save(room);
    }
}
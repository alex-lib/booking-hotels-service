package com.service.bookinghotels.services;
import com.service.bookinghotels.entities.Booking;
import org.springframework.security.core.userdetails.UserDetails;
import java.util.List;

public interface BookingService {

    Booking createBooking(Booking booking, UserDetails user);

    Booking updateBooking(Long bookingId, Booking booking);

    void deleteBooking(Long bookingId);

    Booking getBookingById(Long bookingId);

    List<Booking> getAllBookings();
}
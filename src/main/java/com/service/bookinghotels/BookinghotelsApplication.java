package com.service.bookinghotels;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.starter.outbox.jpa.OutboxEntity;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.service.bookinghotels.repositories")
@EntityScan(basePackages = "com.service.bookinghotels.entities",
        basePackageClasses = OutboxEntity.class)
public class BookinghotelsApplication {

	public static void main(String[] args) {
		SpringApplication.run(BookinghotelsApplication.class, args);
	}
}
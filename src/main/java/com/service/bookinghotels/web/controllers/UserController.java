package com.service.bookinghotels.web.controllers;
import com.service.bookinghotels.aop.user.CheckUserAccess;
import com.service.bookinghotels.entities.User;
import com.service.bookinghotels.entities.roles.RoleType;
import com.service.bookinghotels.mappers.user.UserMapper;
import com.service.bookinghotels.services.UserService;
import com.service.bookinghotels.web.dto.user.UserRequest;
import com.service.bookinghotels.web.dto.user.UserResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    private final UserMapper userMapper;

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping
    public UserResponse createUser(@RequestBody @Valid UserRequest userRequest,
                                   @RequestParam RoleType roleType) {
        User newUser = userService.createUser(userMapper.userRequestToUser(userRequest), roleType);
        return userMapper.userToUserResponse(newUser);
    }

    @ResponseStatus(HttpStatus.OK)
    @GetMapping("/{id}")
    @PreAuthorize("hasAnyAuthority('ROLE_USER', 'ROLE_ADMIN')")
    @CheckUserAccess
    public UserResponse findUserById(@PathVariable Long id,
                                     @AuthenticationPrincipal UserDetails user) {
        return userMapper.userToUserResponse(userService
                .getUserById(id));
    }

    @ResponseStatus(HttpStatus.OK)
    @PutMapping("/{id}")
    @PreAuthorize("hasAnyAuthority('ROLE_USER', 'ROLE_ADMIN')")
    @CheckUserAccess
    public UserResponse updateUser(@PathVariable Long id,
                                   @AuthenticationPrincipal UserDetails user,
                                   @RequestBody @Valid UserRequest userRequest) {
        return userMapper.userToUserResponse(userService
                .updateUser(id, userMapper.userRequestToUser(id, userRequest)));
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/{id}")
    @PreAuthorize("hasAnyAuthority('ROLE_USER', 'ROLE_ADMIN')")
    @CheckUserAccess
    public void deleteUser(@PathVariable Long id,
                           @AuthenticationPrincipal UserDetails user) {
        userService.deleteUser(id);
    }
}
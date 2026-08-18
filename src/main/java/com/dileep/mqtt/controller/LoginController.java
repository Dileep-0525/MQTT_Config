package com.dileep.mqtt.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dileep.mqtt.dto.LoginRequest;
import com.dileep.mqtt.util.JwtUtil;

import lombok.RequiredArgsConstructor;
@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class LoginController {

	private final JwtUtil jwtUtil;
	
	 @PostMapping("/login")
	    public String Login(@RequestBody LoginRequest request) {
	        return jwtUtil.generateToken(1l,request.username());
	    }
	
}

package com.dileep.mqtt.util;

import javax.crypto.SecretKey;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

@Component
public class JwtUtil {

	@Value("${jwt.secret}")
	private String jwtSecret;

	private SecretKey getSigningKey() {

		return Keys.hmacShaKeyFor(jwtSecret.getBytes());
	}

	public boolean validateToken(String token) {

		try {

			Jwts.parser().verifyWith(getSigningKey()).build().parseSignedClaims(token);

			return true;

		} catch (Exception e) {

			return false;
		}
	}

	public String extractUsername(String token) {

		Claims claims = Jwts.parser().verifyWith(getSigningKey()).build().parseSignedClaims(token).getPayload();

		return claims.getSubject();
	}

//	public String generateToken(String username) {
//
//		return Jwts.builder().subject(username).signWith(getSigningKey()).compact();
//		//		 return Jwts.builder()
//		//		            .subject(username)
//		//		            .issuedAt(new Date())
//		//		            .expiration(
//		//		                    new Date(System.currentTimeMillis() + 15 * 60 * 1000)
//		//		            )
//		//		            .signWith(getSigningKey())
//		//		            .compact();
//	}

	public String generateToken(Long userId, String username) {

		return Jwts.builder().subject(username).claim("userId", userId).signWith(getSigningKey()).compact();
	}
	
	public Long extractUserId(String token) {

		Claims claims = Jwts.parser().verifyWith(getSigningKey()).build().parseSignedClaims(token).getPayload();

		return claims.get("userId", Long.class);
	}
	
}
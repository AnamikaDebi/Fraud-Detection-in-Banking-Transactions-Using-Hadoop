package com.pack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to calculate distance between two zip codes using their latitude and longitude.
 * Reads zip code data from a CSV file (zipCodePosId.csv).
 */
class ZipCodeData {
    double lat;
    double lon;
    String city;
    String state;
    String postId;

    public ZipCodeData(double lat, double lon, String city, String state, String postId) {
        this.lat = lat;
        this.lon = lon;
        this.city = city;
        this.state = state;
        this.postId = postId;
    }
}

public class DistanceUtility {
    private static DistanceUtility instance;
    private final Map<String, ZipCodeData> zipCodesMap = new HashMap<>();

    // Singleton pattern to ensure one instance
    public static synchronized DistanceUtility getInstance(String zipCodeCsvPath) throws IOException {
        if (instance == null) {
            instance = new DistanceUtility(zipCodeCsvPath);
        }
        return instance;
    }

    private DistanceUtility(String zipCodeCsvPath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(zipCodeCsvPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                if (data.length != 6) continue; // Skip malformed lines

                String zipCode = data[0];
                double lat = Double.parseDouble(data[1]);
                double lon = Double.parseDouble(data[2]);
                String city = data[3];
                String state = data[4];
                String postId = data[5];

                zipCodesMap.put(zipCode, new ZipCodeData(lat, lon, city, state, postId));
            }
        } catch (NumberFormatException e) {
            throw new IOException("Invalid numeric data in CSV file", e);
        }
    }

    public double getDistanceViaZipCode(String zipCode1, String zipCode2) {
        ZipCodeData z1 = zipCodesMap.get(zipCode1);
        ZipCodeData z2 = zipCodesMap.get(zipCode2);
        if (z1 == null || z2 == null) {
            throw new IllegalArgumentException("Invalid zip code: " + (z1 == null ? zipCode1 : zipCode2));
        }
        return calculateDistance(z1.lat, z1.lon, z2.lat, z2.lon);
    }

    // Calculate distance using the Haversine formula (in kilometers)
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double lonDiff = Math.toRadians(lon1 - lon2);
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);

        double dist = Math.sin(lat1Rad) * Math.sin(lat2Rad) +
                      Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.cos(lonDiff);
        dist = Math.acos(dist) * 6371; // Earth's radius in km
        return dist;
    }
}

package com.tlink.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Properties;

public class PropertiesUtil {
	public static int getInt(Properties config, String key, int defaultValue) {
		String val = config.getProperty(key);
		if (val == null) {
			return defaultValue;
		} else {
			try {
				return Integer.parseInt(val);
			} catch (NumberFormatException nfe) {
				throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
						"Entered value='" + val + "'. Default value='" + defaultValue + "'");
			}
		}
	}

	public static long getLong(Properties config, String key, long defaultValue) {
		String val = config.getProperty(key);
		if (val == null) {
			return defaultValue;
		} else {
			try {
				return Long.parseLong(val);
			} catch (NumberFormatException nfe) {
				throw new IllegalArgumentException("Value for configuration key='" + key + "' is not set correctly. " +
						"Entered value='" + val + "'. Default value='" + defaultValue + "'");
			}
		}
	}

	public static long getLong(Properties config, String key, long defaultValue, Logger logger) {
		try {
			return getLong(config, key, defaultValue);
		} catch (IllegalArgumentException iae) {
			logger.warn(iae.getMessage());
			return defaultValue;
		}
	}

	public static boolean getBoolean(Properties config, String key, boolean defaultValue) {
		String val = config.getProperty(key);
		if (val == null) {
			return defaultValue;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	public static String[] getStringArray(Properties config, String key) {
		return getStringArray(config, key, new String[0], false);
	}

	public static String[] getStringArray(Properties config, String key, String[] defaultValue) {
		return getStringArray(config, key, defaultValue, false);
	}

	public static String[] getStringArray(Properties config, String key, boolean upperCase) {
		return getStringArray(config, key, new String[0], upperCase);
	}

	public static String[] getStringArray(Properties config, String key, String[] defaultValue, boolean upperCase) {
		String val = config.getProperty(key);

		if (upperCase){
			val = StringUtils.upperCase(val);
		}

		if (StringUtils.isEmpty(val)) {
			return defaultValue;
		} else {
			return StringUtils.remove(val, " ").split(",");
		}
	}

	private PropertiesUtil() {}
}

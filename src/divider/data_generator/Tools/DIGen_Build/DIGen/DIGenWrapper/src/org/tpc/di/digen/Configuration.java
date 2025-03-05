package org.tpc.di.digen;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Configuration
{
  private static final String BUNDLE_NAME = "org.tpc.di.digen.digen";
  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

  public static String getString(String key)
  {
    try
    {
      return RESOURCE_BUNDLE.getString(key); } catch (MissingResourceException e) {
    }
    return '!' + key + '!';
  }
}
//  (c) Copyright 2015 Cloudera, Inc.

package com.cloudera.director.aws.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.common.ResourceBundleLocalizationContext.ResourceBundleLocalizationContextFactory;
import com.cloudera.director.aws.common.ResourceBundleLocalizationContext.ResourceBundleResolver;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.google.common.collect.Maps;

import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.junit.Test;

/**
 * Tests ResourceBundleLocalizationContext.
 */
public class ResourceBundleLocalizationContextTest {

  @Test
  public void testWithResourceBundle() {
    MockStringResourceBundle resourceBundle = new MockStringResourceBundle();
    resourceBundle.put("x", "x-ray");
    resourceBundle.put("x.y", "xylophone");
    resourceBundle.put("x.y.z", "eggzaktly");
    ResourceBundleLocalizationContext localizationContext =
        new ResourceBundleLocalizationContext(Locale.getDefault(), "x", resourceBundle);
    assertThat(localizationContext.localize(null)).isNull();
    assertThat(localizationContext.localize("")).isEqualTo("");
    assertThat(localizationContext.localize("foo")).isEqualTo("foo");
    assertThat(localizationContext.localize("y")).isEqualTo("y");
    assertThat(localizationContext.localize("yam", "y")).isEqualTo("xylophone");
    assertThat(localizationContext.localize("yam", "y", "z")).isEqualTo("eggzaktly");
    assertThat(localizationContext.localize("yam", "z")).isEqualTo("yam");
  }

  @Test
  public void testWithResourceNoBundle() {
    ResourceBundleLocalizationContext localizationContext =
        new ResourceBundleLocalizationContext(Locale.getDefault(), "x", null);
    assertThat(localizationContext.localize(null)).isNull();
    assertThat(localizationContext.localize("")).isEqualTo("");
    assertThat(localizationContext.localize("foo")).isEqualTo("foo");
    assertThat(localizationContext.localize("y")).isEqualTo("y");
    assertThat(localizationContext.localize("yam", "y")).isEqualTo("yam");
    assertThat(localizationContext.localize("yam", "y", "z")).isEqualTo("yam");
    assertThat(localizationContext.localize("yam", "z")).isEqualTo("yam");
  }

  @Test
  public void testLocalization() {
    Locale locale1 = new Locale("en");
    MockStringResourceBundle resourceBundle1 = new MockStringResourceBundle();
    resourceBundle1.put("y", "xylophone");
    resourceBundle1.put("y.z", "eggzaktly");

    Locale locale2 = new Locale("fr");
    MockStringResourceBundle resourceBundle2 = new MockStringResourceBundle(resourceBundle1);
    resourceBundle2.put("y.z", "not eggzaktly");

    ResourceBundleResolver resolver = mock(ResourceBundleResolver.class);
    when(resolver.getBundle(anyString(), eq(locale1))).thenReturn(resourceBundle1);
    when(resolver.getBundle(anyString(), eq(locale2))).thenReturn(resourceBundle2);

    ResourceBundleLocalizationContextFactory factory =
        new ResourceBundleLocalizationContextFactory(resolver);

    LocalizationContext localizationContext1 = factory.createRootLocalizationContext(locale1);
    assertThat(localizationContext1.localize("yam", "y")).isEqualTo("xylophone");
    assertThat(localizationContext1.localize("yam", "y", "z")).isEqualTo("eggzaktly");
    assertThat(localizationContext1.localize("yam", "z")).isEqualTo("yam");

    LocalizationContext localizationContext2 = factory.createRootLocalizationContext(locale2);
    assertThat(localizationContext2.localize("yam", "y")).isEqualTo("xylophone");
    assertThat(localizationContext2.localize("yam", "y", "z")).isEqualTo("not eggzaktly");
    assertThat(localizationContext2.localize("yam", "z")).isEqualTo("yam");
  }

  private static class MockStringResourceBundle extends ResourceBundle {

    private Map<String, String> mappedStrings = Maps.newHashMap();

    private MockStringResourceBundle() {
    }

    private MockStringResourceBundle(ResourceBundle parent) {
      super.setParent(parent);
    }

    private void put(String key, String value) {
      mappedStrings.put(key, value);
    }

    @Override
    protected Object handleGetObject(String key) {
      return mappedStrings.get(key);
    }

    @Override
    public Enumeration<String> getKeys() {
      throw new UnsupportedOperationException();
    }
  }
}

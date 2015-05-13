// (c) Copyright 2015 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.director.aws.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.support.ResourcePropertySource;

/**
 * A convenience class for creating property resolvers.
 */
public final class PropertyResolvers {

  private static final String BUILT_IN_NAME = "built-in";
  private static final String CUSTOM_NAME_PREFIX = "custom";

  private PropertyResolvers() {
  }

  /**
   * Creates a new property resolver with no properties.
   *
   * @return new property resolver
   */
  public static PropertyResolver newEmptyPropertyResolver() {
    return new PropertySourcesPropertyResolver(new MutablePropertySources());
  }

  /**
   * Creates a new property resolver that gets properties from the given map.
   *
   * @param m property map
   * @return new property resolver
   * @throws NullPointerException if the map is null
   */
  public static PropertyResolver newMapPropertyResolver(Map<String, String> m) {
    checkNotNull(m, "map is null");
    MutablePropertySources sources = new MutablePropertySources();
    sources.addFirst(new MapPropertySource("map",
        ImmutableMap.<String, Object>copyOf(m)));
    return new PropertySourcesPropertyResolver(sources);
  }

  /**
   * Creates a property resolver that pulls from multiple property resource
   * locations. The first "built-in" location must be successfully loaded, but
   * all other "custom" locations may fail to load.
   *
   * @param builtInResourceLocation lowest precedence, required resource
   *                                location for properties
   * @param customResourceLocations additional resource locations for
   *                                properties, in increasing order of precedence
   * @return new property resolver
   * @throws IOException          if the built-in resource location could not be loaded
   * @throws NullPointerException if any resource location is null
   */
  public static PropertyResolver newMultiResourcePropertyResolver(String builtInResourceLocation,
      String... customResourceLocations)
      throws IOException {
    return newMultiResourcePropertyResolver(true, builtInResourceLocation,
        customResourceLocations);
  }

  /**
   * Creates a property resolver that pulls from multiple property resource
   * locations. The first "built-in" location must be successfully loaded, but
   * all other "custom" locations must load only if {code}allowMissing{code} is
   * false.
   *
   * @param allowMissing            true to allow custom resource locations to fail to load
   * @param builtInResourceLocation lowest precedence, required resource
   *                                location for properties
   * @param customResourceLocations additional resource locations for
   *                                properties, in increasing order of precedence
   * @return new property resolver
   * @throws IOException          if the built-in resource location could not be loaded,
   *                              or if {code}allowMissing{code} is false and any custom resource location
   *                              fails to load
   * @throws NullPointerException if any resource location is null
   */
  public static PropertyResolver newMultiResourcePropertyResolver(boolean allowMissing,
      String builtInResourceLocation,
      String... customResourceLocations)
      throws IOException {
    MutablePropertySources sources = new MutablePropertySources();
    checkNotNull(builtInResourceLocation, "builtInResourceLocation is null");
    sources.addLast(buildPropertySource(BUILT_IN_NAME, builtInResourceLocation,
        false));
    String lastname = BUILT_IN_NAME;
    int customCtr = 1;
    for (String loc : customResourceLocations) {
      checkNotNull(loc, "customResourceLocations[" + (customCtr - 1) +
          "] is null");
      String thisname = CUSTOM_NAME_PREFIX + customCtr++;
      PropertySource source = buildPropertySource(thisname, loc, allowMissing);
      if (source != null) {
        sources.addBefore(lastname, source);
        lastname = thisname;
      }
    }

    return new PropertySourcesPropertyResolver(sources);
  }

  private static PropertySource buildPropertySource(String name, String loc,
      boolean allowMissing)
      throws IOException {
    try {
      return new ResourcePropertySource(name, loc,
                                        PropertyResolvers.class.getClassLoader());
    } catch (IOException e) {
      if (allowMissing) {
        return null;
      }
      throw new IOException("Unable to load " + name +
          " properties from " + loc, e);
    }
  }

}

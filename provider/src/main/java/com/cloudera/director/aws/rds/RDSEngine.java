//  (c) Copyright 2015 Cloudera, Inc.

package com.cloudera.director.aws.rds;

import com.cloudera.director.spi.v2.database.DatabaseType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The database engines supported by RDS.
 */
public enum RDSEngine {

  /**
   * The MySQL engine.
   */
  MYSQL(DatabaseType.MYSQL, "MYSQL", "MySQL", true),

  /**
   * The MariaDB engine.
   */
  MARIADB(DatabaseType.MYSQL, "MARIADB", "MariaDB", false);

  /**
   * The error message for an invalid engine.
   */
  static final String INVALID_ENGINE;

  /**
   * The map from supported engine name to supported engine.
   */
  private static final Map<String, RDSEngine> SUPPORTED_ENGINES_BY_ENGINE_NAME;

  /**
   * The list of supported engine names.
   */
  private static final List<String> SUPPORTED_ENGINE_NAMES;

  /**
   * The supported engine names for each database type.
   */
  private static final Multimap<DatabaseType, String> SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE;

  /**
   * The default engine for each database type.
   */
  private static final Map<DatabaseType, RDSEngine> DEFAULT_ENGINES_BY_DATABASE_TYPE;

  static {
    ImmutableMap.Builder<String, RDSEngine> enginesByName = ImmutableMap.builder();
    ImmutableList.Builder<String> engineNames = ImmutableList.builder();
    ImmutableListMultimap.Builder<DatabaseType, String> namesByType =
        ImmutableListMultimap.builder();
    ImmutableMap.Builder<DatabaseType, RDSEngine> defaultEnginesByType =
      ImmutableMap.builder();
    for (RDSEngine engine : values()) {
      String engineName = engine.getEngineName();
      enginesByName.put(engineName, engine);
      engineNames.add(engineName);
      DatabaseType databaseType = engine.getDatabaseType();
      namesByType.put(databaseType, engineName);
      if (engine.isDefaultForType()) {
        defaultEnginesByType.put(databaseType, engine);
      }
    }
    SUPPORTED_ENGINES_BY_ENGINE_NAME = enginesByName.build();
    SUPPORTED_ENGINE_NAMES = engineNames.build();
    SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE = namesByType.build();
    DEFAULT_ENGINES_BY_DATABASE_TYPE = defaultEnginesByType.build();
    INVALID_ENGINE = "Invalid engine (not one of " + SUPPORTED_ENGINE_NAMES + ") : %s";
  }

  /**
   * Returns the map from supported engine name to supported engine.
   *
   * @return the map from supported engine name to supported engine
   */
  public static Map<String, RDSEngine> getSupportedEnginesByEngineName() {
    return SUPPORTED_ENGINES_BY_ENGINE_NAME;
  }

  /**
   * Returns the list of supported engine names.
   *
   * @return the list of supported engine names
   */
  public static List<String> getSupportedEngineNames() {
    return SUPPORTED_ENGINE_NAMES;
  }

  /**
   * Returns the valid supported names for each database type.
   *
   * @return the valid supported names for each database type
   */
  public static Multimap<DatabaseType, String> getSupportedEngineNamesByDatabaseType() {
    return SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE;
  }

  /**
   * Returns the supported engine with the specified name.
   *
   * @param engineName the engine name
   * @return the supported engine with the specified name
   * @throws IllegalArgumentException if there is no such supported engine
   */
  public static RDSEngine getSupportedEngineByEngineName(String engineName) {
    if (!SUPPORTED_ENGINES_BY_ENGINE_NAME.containsKey(engineName)) {
      throw new IllegalArgumentException(String.format(INVALID_ENGINE, engineName));
    }
    return SUPPORTED_ENGINES_BY_ENGINE_NAME.get(engineName);
  }

  /**
   * Returns the supported database engine names for the specified database type.
   *
   * @param databaseType the database type
   * @return the supported database engine names for the specified database type
   */
  public static Collection<String> getSupportedEngineNames(DatabaseType databaseType) {
    if (!SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE.containsKey(databaseType)) {
      throw new IllegalArgumentException("Unsupported database type " + databaseType
          + "(not one of " + SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE.keySet() + ")");
    }
    return SUPPORTED_ENGINE_NAMES_BY_DATABASE_TYPE.get(databaseType);
  }

  /**
   * Returns the supported database types.
   *
   * @return the supported database types
   */
  public static Set<DatabaseType> getSupportedDatabaseTypes() {
    return getSupportedEngineNamesByDatabaseType().keySet();
  }

  /**
   * Returns the default database engine for the specified database type.
   *
   * @param databaseType the database type
   * @return the default database engine for the specified database type
   */
  public static RDSEngine getDefaultEngine(DatabaseType databaseType) {
    if (!DEFAULT_ENGINES_BY_DATABASE_TYPE.containsKey(databaseType)) {
      throw new IllegalArgumentException("Unsupported database type " + databaseType
          + "(not one of " + DEFAULT_ENGINES_BY_DATABASE_TYPE.keySet() + ")");
    }
    return DEFAULT_ENGINES_BY_DATABASE_TYPE.get(databaseType);
  }

  /**
   * Returns the supported database engine names, as an array for the convenience of the
   * configuration property builder.
   *
   * @return the supported database engine names
   */
  static String[] getSupportedEngineNamesAsArray() {
    List<String> supportedEngineNames = getSupportedEngineNames();
    return supportedEngineNames.toArray(new String[supportedEngineNames.size()]);
  }

  /**
   * Returns the supported database engine names, as an array for the convenience of the
   * configuration property builder.
   *
   * @return the supported database engine names
   */
  static String[] getSupportedDatabaseTypeNamesAsArray() {
    List<String> supportedDatabaseTypeNames = Lists.newArrayList();
    for (DatabaseType databaseType : getSupportedDatabaseTypes()) {
      supportedDatabaseTypeNames.add(databaseType.name());
    }
    return supportedDatabaseTypeNames.toArray(new String[supportedDatabaseTypeNames.size()]);
  }

  /**
   * The generic database type for the engine.
   */
  private final DatabaseType databaseType;

  /**
   * The engine name, as required by RDS.
   */
  private final String engineName;

  /**
   * The default human-readable description of the engine, used when a
   * localized description cannot be found.
   */
  private final String defaultDescription;

  /**
   * Whether this engine is the default engine for its type.
   */
  private final boolean defaultForType;

  /**
   * Creates a supported engine with the specified parameters.
   *
   * @param databaseType       the generic database type for the engine
   * @param engineName         the engine name, as required by RDS
   * @param defaultDescription the default human-readable description of the engine, used when a
   *                           localized description cannot be found
   */
  RDSEngine(DatabaseType databaseType, String engineName, String defaultDescription,
      boolean defaultForType) {
    this.databaseType = databaseType;
    this.defaultDescription = defaultDescription;
    this.engineName = engineName;
    this.defaultForType = defaultForType;
  }

  /**
   * Returns the generic database type for the engine.
   *
   * @return the generic database type for the engine
   */
  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  /**
   * Returns the engine name, as required by RDS.
   *
   * @return the engine name, as required by RDS
   */
  public String getEngineName() {
    return engineName;
  }

  /**
   * Returns the default human-readable description of the engine, used when a
   * localized description cannot be found.
   *
   * @return the default human-readable description of the engine, used when a
   * localized description cannot be found
   */
  public String getDefaultDescription() {
    return defaultDescription;
  }

  /**
   * Returns whether the engine is the default engine for its database type.
   *
   * @return whether the engine is the default engine for its database type
   */
  public boolean isDefaultForType() {
    return defaultForType;
  }
}

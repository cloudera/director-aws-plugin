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

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.ASSOCIATE_PUBLIC_IP_ADDRESSES;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.IAM_ENDPOINT;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.aws.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION_ENDPOINT;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PRIVATE_KEY;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PUBLIC_KEY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Region;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.SpotInstanceState;
import com.amazonaws.services.ec2.model.SpotPlacement;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.InstanceTags;
import com.cloudera.director.aws.Tags;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.Property;
import com.cloudera.director.spi.v1.model.Resource;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v1.model.exception.TransientProviderException;
import com.cloudera.director.spi.v1.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v1.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.cloudera.director.spi.v1.util.KeySerialization;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compute provider of Amazon EC2 instances.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class EC2Provider extends AbstractComputeProvider<EC2Instance, EC2InstanceTemplate> {

  private static final Logger LOG = LoggerFactory.getLogger(EC2Provider.class);

  /**
   * The provider configuration properties.
   */
  protected static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil.asConfigurationPropertyList(
          EC2ProviderConfigurationPropertyToken.values());

  /**
   * EC2 only allows 95 instance status requests per batch.
   */
  private static final int MAX_INSTANCE_STATUS_REQUESTS = 95;

  /**
   * The resource provider ID.
   */
  public static final String ID = EC2Provider.class.getCanonicalName();

  /**
   * The resource provider metadata.
   */
  public static final ResourceProviderMetadata METADATA = SimpleResourceProviderMetadata.builder()
      .id(ID)
      .name("EC2 (Elastic Compute Cloud)")
      .description("AWS EC2 compute provider")
      .providerClass(EC2Provider.class)
      .providerConfigurationProperties(CONFIGURATION_PROPERTIES)
      .resourceTemplateConfigurationProperties(EC2InstanceTemplate.getConfigurationProperties())
      .resourceDisplayProperties(EC2Instance.getDisplayProperties())
      .build();

  /**
   * The default spot instance request duration, in milliseconds.
   */
  public static final int DEFAULT_SPOT_INSTANCE_REQUEST_DURATION_MS = 10 * 60 * 1000; //10 min

  /**
   * The default amount of time to wait, in milliseconds, for a Spot price change when the Spot
   * bid is known to be below the current Spot price.
   */
  public static final int DEFAULT_SPOT_INSTANCE_PRICE_CHANGE_DURATION_MS = 0;

  /**
   * EC2 configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public enum EC2ProviderConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

    /**
     * Whether to associate a public IP address with instances. Default is <code>true</code>.
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/vpc-ip-addressing.html">IP Addressing in your VPC</a>
     */
    ASSOCIATE_PUBLIC_IP_ADDRESSES(new SimpleConfigurationPropertyBuilder()
        .configKey("associatePublicIpAddresses")
        .name("Associate public IP addresses")
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultValue("true")
        .type(Property.Type.BOOLEAN)
        .defaultDescription("Whether to associate a public IP address with instances or not. " +
            "If this is false, instances are expected to be able to access the internet using a NAT instance. " +
            "Currently the only way to get optimal S3 data transfer performance is to assign " +
            "public IP addresses to instances and not use NAT instances (public subnet setup).")
        .build()),

    IAM_ENDPOINT(new SimpleConfigurationPropertyBuilder()
        .configKey("iamEndpoint")
        .name("IAM endpoint")
        .defaultDescription("<p>IAM endpoint is an optional URL that Cloudera Director can use to communicate with the AWS Identity and Access Management service.  AWS provides a single endpoint for IAM.</p>For more information see the <a target=\"_blank\" href=\"http://docs.aws.amazon.com/general/latest/gr/rande.html#iam_region\">AWS documentation.</a>")
        .build()),

    /**
     * EC2 region. Each region is a separate geographic area. Each region has multiple, isolated
     * locations known as Availability Zones. Default is us-east-1.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html">Regions and Availability Zones</a>
     */
    REGION(new SimpleConfigurationPropertyBuilder()
        .configKey("region")
        .name("EC2 region")
        .defaultValue("us-east-1")
        .defaultDescription("ID of the Amazon Web Services region to use.")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .addValidValues(
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-southeast-1",
            "ap-southeast-2",
            "eu-central-1",
            "eu-west-1",
            "sa-east-1",
            "us-east-1",
            "us-west-1",
            "us-west-2")
        .build()),

    /**
     * <p>Custom endpoint identifying a region.</p>
     * <p>This is critical for Gov. cloud because there is no other way to discover those
     * regions.</p>
     */
    REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
        .configKey("regionEndpoint")
        .name("EC2 region endpoint")
        .defaultDescription("<p>EC2 region endpoint is an optional URL that Cloudera Director can use to communicate with the AWS EC2 service.  AWS provides multiple regional endpoints for EC2 as well as GovCloud endpoints.</p>For more information see the <a target=\"_blank\" href=\"http://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region\">AWS documentation.</a>")
        .build());

    /**
     * The configuration property.
     */
    private final ConfigurationProperty configurationProperty;

    /**
     * Creates a configuration property token with the specified parameters.
     *
     * @param configurationProperty the configuration property
     */
    EC2ProviderConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  /**
   * Configures the specified EC2 client.
   *
   * @param configuration               the provider configuration
   * @param accumulator                 the exception accumulator
   * @param client                      the EC2 client
   * @param providerLocalizationContext the resource provider localization context
   * @param verify                      whether to verify the configuration by making an API call
   * @return the configured client
   * @throws InvalidCredentialsException    if the supplied credentials are invalid
   * @throws TransientProviderException     if a transient exception occurs communicating with the
   *                                        provider
   * @throws UnrecoverableProviderException if an unrecoverable exception occurs communicating with
   *                                        the provider
   */
  protected static AmazonEC2Client configureClient(Configured configuration,
      PluginExceptionConditionAccumulator accumulator, AmazonEC2Client client,
      LocalizationContext providerLocalizationContext, boolean verify) {
    checkNotNull(client, "client is null");

    try {
      String regionEndpoint =
          configuration.getConfigurationValue(REGION_ENDPOINT, providerLocalizationContext);
      if (regionEndpoint != null) {
        LOG.info("<< Using configured region endpoint: {}", regionEndpoint);
      } else {
        String region = configuration.getConfigurationValue(REGION, providerLocalizationContext);
        regionEndpoint = getEndpointForRegion(client, region);
      }
      client.setEndpoint(regionEndpoint);

      if (verify) {
        // Attempt to use client, to validate credentials and connectivity
        client.describeRegions();
      }

    } catch (AmazonClientException e) {
      throw AWSExceptions.propagate(e);
    } catch (IllegalArgumentException e) {
      accumulator.addError(REGION.unwrap().getConfigKey(), e.getMessage());
    }
    return client;
  }

  /**
   * Configures the specified IAM client.
   *
   * @param configuration               the provider configuration
   * @param accumulator                 the exception accumulator
   * @param identityManagementClient    the IAM client
   * @param providerLocalizationContext the resource provider localization context
   * @param verify                      whether to verify the configuration by making an API call
   * @return the configured client
   * @throws InvalidCredentialsException    if the supplied credentials are invalid
   * @throws TransientProviderException     if a transient exception occurs communicating with the
   *                                        provider
   * @throws UnrecoverableProviderException if an unrecoverable exception occurs communicating with
   *                                        the provider
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  protected static AmazonIdentityManagementClient configureIAMClient(Configured configuration,
      PluginExceptionConditionAccumulator accumulator,
      AmazonIdentityManagementClient identityManagementClient,
      LocalizationContext providerLocalizationContext, boolean verify) {
    checkNotNull(identityManagementClient, "identityManagementClient is null");

    try {
      String iamEndpoint =
          configuration.getConfigurationValue(IAM_ENDPOINT, providerLocalizationContext);
      if (iamEndpoint != null) {
        LOG.info("<< Using configured IAM endpoint: {}", iamEndpoint);
        identityManagementClient.setEndpoint(iamEndpoint);
      }
      // else use the single default endpoint for all of AWS (outside GovCloud)

      if (verify) {
        // Attempt to use client, to validate credentials and connectivity
        try {
          identityManagementClient.getInstanceProfile(
              new GetInstanceProfileRequest().withInstanceProfileName("test"));
        } catch (NoSuchEntityException e) {
          /* call succeeded */
        }
      }

    } catch (AmazonClientException e) {
      throw AWSExceptions.propagate(e);
    } catch (IllegalArgumentException e) {
      accumulator.addError(IAM_ENDPOINT.unwrap().getConfigKey(), e.getMessage());
    }
    return identityManagementClient;
  }

  /**
   * Returns the endpoint URL for the specified region.
   *
   * @param client     the EC2 client
   * @param regionName the desired region
   * @return the endpoint URL for the specified region
   * @throws IllegalArgumentException if the endpoint cannot be determined
   */
  private static String getEndpointForRegion(AmazonEC2Client client, String regionName) {
    checkNotNull(client, "client is null");
    checkNotNull(regionName, "regionName is null");

    LOG.info(">> Describing all regions to find endpoint for '{}'", regionName);

    DescribeRegionsResult result = client.describeRegions();
    List<String> regions = Lists.newArrayListWithExpectedSize(result.getRegions().size());

    for (Region candidate : result.getRegions()) {
      regions.add(candidate.getRegionName());

      if (candidate.getRegionName().equals(regionName)) {
        LOG.info("<< Found endpoint '{}' for region '{}'", candidate.getEndpoint(), regionName);

        return candidate.getEndpoint();
      }
    }

    throw new IllegalArgumentException(String.format("Unable to find an endpoint for region '%s'. "
        + "Choose one of the following regions: %s", regionName, Joiner.on(", ").join(regions)));
  }

  private final AmazonEC2Client client;
  private final AmazonIdentityManagementClient identityManagementClient;

  private final EphemeralDeviceMappings ephemeralDeviceMappings;
  private final VirtualizationMappings virtualizationMappings;
  private final AWSFilters ec2Filters;

  private final boolean associatePublicIpAddresses;

  private final ConfigurationValidator resourceTemplateConfigurationValidator;

  /**
   * Construct a new provider instance and validate all configurations.
   *
   * @param configuration            the configuration
   * @param ephemeralDeviceMappings  the ephemeral device mappings
   * @param virtualizationMappings   the virtualization mappings
   * @param awsFilters               the AWS filters
   * @param client                   the EC2 client
   * @param identityManagementClient the IAM client
   * @param cloudLocalizationContext the parent cloud localization context
   * @throws InvalidCredentialsException    if the supplied credentials are invalid
   * @throws TransientProviderException     if a transient exception occurs communicating with the
   *                                        provider
   * @throws UnrecoverableProviderException if an unrecoverable exception occurs communicating with
   *                                        the provider
   */
  public EC2Provider(Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      VirtualizationMappings virtualizationMappings, AWSFilters awsFilters, AmazonEC2Client client,
      AmazonIdentityManagementClient identityManagementClient,
      LocalizationContext cloudLocalizationContext) {
    super(configuration, METADATA, cloudLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();
    this.ephemeralDeviceMappings =
        checkNotNull(ephemeralDeviceMappings, "ephemeralDeviceMappings is null");
    this.virtualizationMappings =
        checkNotNull(virtualizationMappings, "virtualizationMappings is null");
    this.ec2Filters = checkNotNull(awsFilters, "awsFilters").getSubfilters(ID);

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();
    this.client = configureClient(configuration, accumulator, client, localizationContext, false);
    this.identityManagementClient = configureIAMClient(configuration, accumulator,
        identityManagementClient, localizationContext, false);
    if (accumulator.hasError()) {
      PluginExceptionDetails pluginExceptionDetails =
          new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException("Provider initialization failed",
          pluginExceptionDetails);
    }

    this.associatePublicIpAddresses = Boolean.parseBoolean(
        getConfigurationValue(ASSOCIATE_PUBLIC_IP_ADDRESSES, localizationContext));

    this.resourceTemplateConfigurationValidator =
        new CompositeConfigurationValidator(METADATA.getResourceTemplateConfigurationValidator(),
            new EC2InstanceTemplateConfigurationValidator(this));
  }

  public AmazonEC2Client getClient() {
    return client;
  }

  public AmazonIdentityManagementClient getIdentityManagementClient() {
    return identityManagementClient;
  }

  /**
   * Returns the ephemeral device mappings.
   *
   * @return the ephemeral device mappings
   */
  public EphemeralDeviceMappings getEphemeralDeviceMappings() {
    return ephemeralDeviceMappings;
  }

  /**
   * Returns the virtualization mappings.
   *
   * @return the virtualization mappings
   */
  public VirtualizationMappings getVirtualizationMappings() {
    return virtualizationMappings;
  }

  /**
   * Returns the EC2 filters.
   *
   * @return the EC2 filters
   */
  public AWSFilters getEC2Filters() {
    return ec2Filters;
  }

  @Override
  public ConfigurationValidator getResourceTemplateConfigurationValidator() {
    return resourceTemplateConfigurationValidator;
  }

  @Override
  public Resource.Type getResourceType() {
    return EC2Instance.TYPE;
  }

  @Override
  public EC2InstanceTemplate createResourceTemplate(String name, Configured configuration,
      Map<String, String> tags) {

    LocalizationContext providerLocalizationContext = getLocalizationContext();
    LocalizationContext templateLocalizationContext =
        SimpleResourceTemplate.getTemplateLocalizationContext(providerLocalizationContext);
    configuration = enhanceTemplateConfiguration(name, configuration, templateLocalizationContext);

    return new EC2InstanceTemplate(name, configuration, tags, providerLocalizationContext);
  }

  @Override
  public void allocate(EC2InstanceTemplate template, Collection<String> virtualInstanceIds,
      int minCount) throws InterruptedException {
    if (template.isUseSpotInstances()) {
      allocateSpotInstances(template, virtualInstanceIds, minCount);
    } else {
      allocateOnDemandInstances(template, virtualInstanceIds, minCount);
    }
  }

  @Override
  public Collection<EC2Instance> find(final EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds) throws InterruptedException {

    LOG.debug("Finding virtual instances {}", virtualInstanceIds);
    final Collection<EC2Instance> ec2Instances =
        Lists.newArrayListWithExpectedSize(virtualInstanceIds.size());

    forEachInstance(virtualInstanceIds, new InstanceHandler() {
      @Override
      public void handle(Instance instance) {
        String virtualInstanceId = checkInstanceIsManagedByDirector(instance, template);
        ec2Instances.add(new EC2Instance(template, virtualInstanceId, instance));
      }
    });

    LOG.debug("Found {} instances for {} virtual instance IDs", ec2Instances.size(),
        virtualInstanceIds.size());
    return ec2Instances;
  }

  @Override
  @SuppressWarnings("PMD.UnusedFormalParameter")
  public void delete(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds) throws InterruptedException {

    if (virtualInstanceIds.isEmpty()) {
      return;
    }

    Map<String, String> ec2InstanceIdsByVirtualInstanceId =
        getEC2InstanceIdsByVirtualInstanceId(virtualInstanceIds);
    Collection<String> ec2InstanceIds = ec2InstanceIdsByVirtualInstanceId.values();
    if (ec2InstanceIds.isEmpty()) {
      LOG.info("Unable to terminate instances, all unknown {}", virtualInstanceIds);
      return;
    }

    LOG.info(">> Terminating {}", ec2InstanceIds);
    TerminateInstancesResult terminateResult = client.terminateInstances(
        new TerminateInstancesRequest().withInstanceIds(ec2InstanceIds));
    LOG.info("<< Result {}", terminateResult);

    if (ec2InstanceIdsByVirtualInstanceId.size() != virtualInstanceIds.size()) {
      Set<String> missingVirtualInstanceIds = Sets.newLinkedHashSet();
      for (String virtualInstanceId : virtualInstanceIds) {
        if (!ec2InstanceIdsByVirtualInstanceId.containsKey(virtualInstanceId)) {
          missingVirtualInstanceIds.add(virtualInstanceId);
        }
      }
      LOG.info("Unable to terminate unknown instances {}", missingVirtualInstanceIds);
    }
  }

  @Override
  @SuppressWarnings("PMD.UnusedFormalParameter")
  public Map<String, InstanceState> getInstanceState(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds) {
    Map<String, InstanceState> instanceStateByVirtualInstanceId =
        Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());

    // Partition full requests into multiple batch requests, AWS limits
    // the total number of instance status requests you can make.
    List<List<String>> partitions =
        Lists.partition(Lists.newArrayList(virtualInstanceIds), MAX_INSTANCE_STATUS_REQUESTS);

    for (List<String> partition : partitions) {
      instanceStateByVirtualInstanceId.putAll(getBatchInstanceState(partition));
    }

    return instanceStateByVirtualInstanceId;
  }

  @Override
  protected Configured enhanceTemplateConfiguration(String name, Configured configuration,
      LocalizationContext templateLocalizationContext) {
    // Add the key name to the configuration if possible.
    String privateKeyString =
        configuration.getConfigurationValue(SSH_JCE_PRIVATE_KEY, templateLocalizationContext);
    if (privateKeyString != null) {
      String publicKeyString =
          configuration.getConfigurationValue(SSH_JCE_PUBLIC_KEY, templateLocalizationContext);
      configuration = addKeyName(configuration, templateLocalizationContext, privateKeyString,
          publicKeyString);
    } else {
      LOG.warn("No private key fingerprint specified for template {}", name);
    }
    return configuration;
  }

  /**
   * Adds the AWS key name corresponding to a private key to the given
   * configuration.
   *
   * @param configuration    the configuration to be enhanced
   * @param privateKeyString private key, in serialized form
   * @return the enhanced configuration
   * @throws IllegalArgumentException if the key could not be deserialized, or if no key known to
   *                                  AWS matches this key's fingerprint
   */
  private Configured addKeyName(Configured configuration,
      LocalizationContext templateLocalizationContext,
      String privateKeyString, String publicKeyString) {
    PrivateKey privateKey;
    PublicKey publicKey;
    try {
      KeySerialization keySerialization = new KeySerialization();
      privateKey = keySerialization.deserializePrivateKey(privateKeyString);
      if (publicKeyString != null) {
        publicKey = keySerialization.deserializePublicKey(publicKeyString);
      } else {
        publicKey = null;
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to deserialize private key from compute instance template", e);
    }
    String privateKeyFingerprint = getSha1Fingerprint(privateKey);
    String publicKeyFingerprint = getMd5Fingerprint(publicKey);
    String keyName = lookupKeyName(privateKeyFingerprint, publicKeyFingerprint);
    if (keyName == null) {
      throw new IllegalArgumentException("No private key in EC2 matches the fingerprint " +
          privateKeyFingerprint);
    }
    LOG.info("Found EC2 key name {} for fingerprint", keyName);
    Map<String, String> configMap =
        Maps.newHashMap(configuration.getConfiguration(templateLocalizationContext));
    configMap.put(KEY_NAME.unwrap().getConfigKey(),
        keyName);
    return new SimpleConfiguration(configMap);
  }

  /**
   * Gets the SHA-1 digest of the private key bits. This is used as a
   * fingerprint by AWS for a key pair generated by AWS.
   *
   * @param privateKey private key
   * @return private key fingerprint, as a lowercase hex string without colons
   * @throws IllegalStateException if the SHA-1 digest algorithm is not
   *                               available
   */
  private static String getSha1Fingerprint(PrivateKey privateKey) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    byte[] fingerprintBytes = digest.digest(privateKey.getEncoded());
    return DatatypeConverter.printHexBinary(fingerprintBytes).toLowerCase(Locale.US);
  }

  /**
   * Gets the MD5 digest of the public key bits. This is used as a fingerprint
   * by AWS for an imported key pair.
   *
   * @param publicKey public key
   * @return public key fingerprint, as a lowercase hex string without colons,
   * or null if publicKey is null
   */
  private static String getMd5Fingerprint(PublicKey publicKey) {
    if (publicKey == null) {
      return null;
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    byte[] fingerprintBytes = digest.digest(publicKey.getEncoded());
    return DatatypeConverter.printHexBinary(fingerprintBytes).toLowerCase(Locale.US);
  }

  /**
   * Returns the key name corresponding to the specified fingerprints, or {@code null} if it
   * cannot be determined.
   *
   * @param privateKeyFingerprint the private key fingerprint
   * @param publicKeyFingerprint  the public key fingerprint
   * @return the key name corresponding to the specified fingerprints, or {@code null} if it
   * cannot be determined
   */
  private String lookupKeyName(String privateKeyFingerprint, String publicKeyFingerprint) {
    DescribeKeyPairsResult keyPairsResult = client.describeKeyPairs();
    for (KeyPairInfo keyPairInfo : keyPairsResult.getKeyPairs()) {
      String knownFingerprint = keyPairInfo.getKeyFingerprint().replace(":", "");
      LOG.debug("Found fingerprint {} for keyName {}", knownFingerprint, keyPairInfo.getKeyName());
      if (privateKeyFingerprint.equals(knownFingerprint)) {
        return keyPairInfo.getKeyName();
      }
      if (publicKeyFingerprint.equals(knownFingerprint)) {
        return keyPairInfo.getKeyName();
      }
    }
    return null;
  }

  /**
   * Atomically allocates multiple regular EC2 instances with the specified identifiers based on a
   * single instance template. If not all the instances can be allocated, the number of instances
   * allocated must be at least the specified minimum or the method must fail cleanly with no
   * billing implications.
   *
   * @param template           the instance template
   * @param virtualInstanceIds the unique identifiers for the instances
   * @param minCount           the minimum number of instances to allocate if not all resources can
   *                           be allocated
   * @throws InterruptedException if the operation is interrupted
   */
  public void allocateOnDemandInstances(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds, int minCount) throws InterruptedException {
    int instanceCount = virtualInstanceIds.size();

    LOG.info(">> Requesting {} instances for {}", instanceCount, template);

    // EC2 client doesn't accept a min count of 0. Readjust the requested
    // value to 1 to allow submitting the request.
    int normalizedMinCount = (minCount == 0) ? 1 : minCount;

    RunInstancesResult runInstancesResult;
    try {
      runInstancesResult = client.runInstances(
          newRunInstancesRequest(template, virtualInstanceIds, normalizedMinCount));
    } catch (AmazonServiceException e) {
      if (minCount == 0 && "InsufficientInstanceCapacity".equals(e.getErrorCode())) {
        LOG.warn("Ignoring insufficient capacity exception due to min count being zero", e);
        return;
      } else {
        throw e;
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("<< Reservation {} with {}", runInstancesResult.getReservation().getReservationId(),
          summarizeReservationForLogging(runInstancesResult.getReservation()));
    }

    // Tag all the new instances so that we can easily find them later on.
    // Determine which do not yet have a private IP address.

    List<Tag> userDefinedTags = getUserDefinedTags(template);

    Set<String> instancesWithNoPrivateIp = Sets.newHashSet();

    List<Instance> instances = runInstancesResult.getReservation().getInstances();

    // Limit the number of virtual instance id's used for tagging to the
    // number of instances that we managed to reserve.
    List<String> reducedVirtualInstanceIds = FluentIterable
      .from(virtualInstanceIds)
      .limit(instances.size())
      .toList();

    for (Map.Entry<String, Instance> entry : zipWith(reducedVirtualInstanceIds, instances)) {

      String virtualInstanceId = entry.getKey();
      Instance instance = entry.getValue();
      String ec2InstanceId = instance.getInstanceId();

      tagInstance(template, userDefinedTags, virtualInstanceId, ec2InstanceId);

      if (instance.getPrivateIpAddress() == null) {
        instancesWithNoPrivateIp.add(ec2InstanceId);
      } else {
        LOG.info("<< Instance {} got IP {}", ec2InstanceId, instance.getPrivateIpAddress());
      }
    }

    // Wait until all of them have a private IP (it should be pretty fast)
    waitForPrivateIpAddresses(instancesWithNoPrivateIp);
  }

  /**
   * <p>Atomically allocates multiple EC2 Spot Instances with the specified identifiers based on a
   * single instance template. If not all the instances can be allocated, the number of instances
   * allocated must be at least the specified minimum or the method must fail cleanly with no
   * billing implications.</p>
   * <p><em>Note:</em> contrary to the contract of the SPI method, there are some cases where
   * despite failing to satisfy the min count there are billing implications, due to non-atomicity
   * of AWS operations. In particular, if we lose connectivity to AWS for an extended period of time
   * after creating Spot instance requests, we will be unable to cancel the requests, and unable to
   * detect that instances have been provisioned. The resulting requests and/or instances may or
   * may not be tagged appropriately, depending on when connectivity was interrupted.</p>
   *
   * @param template           the instance template
   * @param virtualInstanceIds the unique identifiers for the instances
   * @param minCount           the minimum number of instances to allocate if not all resources can
   *                           be allocated
   * @throws InterruptedException if the operation is interrupted
   */
  public void allocateSpotInstances(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds, int minCount) throws InterruptedException {

    // TODO add configurable duration
    long startTime = System.currentTimeMillis();
    Date requestExpirationTime =
        new Date(startTime + DEFAULT_SPOT_INSTANCE_REQUEST_DURATION_MS);
    Date priceChangeDeadline =
        new Date(startTime + DEFAULT_SPOT_INSTANCE_PRICE_CHANGE_DURATION_MS);

    SpotGroupAllocator spotGroupAllocator =
        new SpotGroupAllocator(
            template, virtualInstanceIds, minCount, requestExpirationTime, priceChangeDeadline);
    spotGroupAllocator.allocate();
  }

  /**
   * Returns a map from virtual instance IDs to instance state for the specified batch of virtual
   * instance IDs.
   *
   * @param virtualInstanceIds batch of virtual instance IDs
   * @return the map from instance IDs to instance state for the specified batch of virtual
   * instance IDs
   */
  private Map<String, InstanceState> getBatchInstanceState(Collection<String> virtualInstanceIds) {
    Map<String, InstanceState> instanceStateByVirtualInstanceId =
        Maps.newHashMapWithExpectedSize(virtualInstanceIds.size());

    BiMap<String, String> virtualInstanceIdsByEC2InstanceId =
        getEC2InstanceIdsByVirtualInstanceId(virtualInstanceIds).inverse();

    int page = 0;
    LOG.info(">> Fetching page {}", page);
    DescribeInstanceStatusResult result = client.describeInstanceStatus(
        new DescribeInstanceStatusRequest()
            .withInstanceIds(virtualInstanceIdsByEC2InstanceId.keySet())
            .withIncludeAllInstances(true)
    );
    LOG.info("<< Result: {}", result);

    while (!result.getInstanceStatuses().isEmpty()) {
      for (InstanceStatus status : result.getInstanceStatuses()) {

        InstanceStateName currentState =
            InstanceStateName.fromValue(status.getInstanceState().getName());
        String ec2InstanceId = status.getInstanceId();
        String virtualInstanceId = virtualInstanceIdsByEC2InstanceId.get(ec2InstanceId);
        InstanceState instanceState = EC2InstanceState.fromInstanceStateName(currentState);
        instanceStateByVirtualInstanceId.put(virtualInstanceId, instanceState);
      }

      String nextToken = result.getNextToken();
      if (nextToken != null) {
        page++;
        LOG.info(">> Fetching page {} using token {}", page, nextToken);
        result = client.describeInstanceStatus(new DescribeInstanceStatusRequest()
            .withNextToken(nextToken));
        LOG.info("<< Result: {}", result);
      } else {
        break;
      }
    }

    return instanceStateByVirtualInstanceId;
  }

  /**
   * Returns the user-defined tags for the specified instance template.
   *
   * @param template the instance template
   * @return the user-defined tags for the specified instance template
   */
  private List<Tag> getUserDefinedTags(EC2InstanceTemplate template) {
    List<Tag> userDefinedTags = Lists.newArrayListWithExpectedSize(template.getTags().size());
    for (Map.Entry<String, String> entry : template.getTags().entrySet()) {
      userDefinedTags.add(new Tag(entry.getKey(), entry.getValue()));
    }
    return userDefinedTags;
  }

  /**
   * Tags an EC2 instance. Expects that the instance already exists or is in the process of
   * being created.
   *
   * @param template          the instance template
   * @param userDefinedTags   the user-defined tags
   * @param virtualInstanceId the virtual instance id
   * @param ec2InstanceId     the EC2 instance id
   * @throws InterruptedException if the operation is interrupted
   */
  private void tagInstance(EC2InstanceTemplate template, List<Tag> userDefinedTags,
      String virtualInstanceId, String ec2InstanceId) throws InterruptedException {
    LOG.info(">> Tagging {} / {}", ec2InstanceId, virtualInstanceId);
    List<Tag> tags = Lists.newArrayList(
        new Tag(InstanceTags.INSTANCE_NAME, String.format("%s-%s",
            template.getInstanceNamePrefix(), virtualInstanceId)),
        new Tag(Tags.CLOUDERA_DIRECTOR_ID, virtualInstanceId),
        new Tag(Tags.CLOUDERA_DIRECTOR_TEMPLATE_NAME, template.getName())
    );
    tags.addAll(userDefinedTags);

    // Wait for the instance to become visible
    while (!instanceExists(ec2InstanceId)) {
      TimeUnit.SECONDS.sleep(5);
    }
    client.createTags(new CreateTagsRequest().withTags(tags).withResources(ec2InstanceId));
  }

  /**
   * Checks whether the specified instance is visible, <em>i.e.</em> can
   * be located by a describe instance status call.
   *
   * @param ec2InstanceId the EC2 instance id
   */
  private boolean instanceExists(String ec2InstanceId) {
    DescribeInstanceStatusResult result = client.describeInstanceStatus(
        new DescribeInstanceStatusRequest()
            .withInstanceIds(ec2InstanceId)
    );

    for (InstanceStatus status : result.getInstanceStatuses()) {
      if (ec2InstanceId.equals(status.getInstanceId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a {@code RunInstancesRequest} starting from a template and a set of
   * virtual IDs.
   *
   * @param template           the instance template
   * @param virtualInstanceIds the virtual instance IDs
   */
  @SuppressWarnings("ConstantConditions")
  private RunInstancesRequest newRunInstancesRequest(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds, int minCount) {

    LOG.info(">> Building instance requests");

    int groupSize = virtualInstanceIds.size();
    String image = template.getImage();
    String type = template.getType();

    InstanceNetworkInterfaceSpecification network =
        getInstanceNetworkInterfaceSpecification(template);

    List<BlockDeviceMapping> deviceMappings = getBlockDeviceMappings(template);

    LOG.info(">> Instance request type: {}, image: {}, group size: {}",
      type, image, groupSize);

    RunInstancesRequest request = new RunInstancesRequest()
        .withImageId(image)
        .withInstanceType(type)
        .withMaxCount(groupSize)
        .withMinCount(minCount)
        .withClientToken(getHashOfVirtualInstanceIdsForClientToken(
            virtualInstanceIds, Optional.<Long>absent()))
        .withNetworkInterfaces(network)
        .withBlockDeviceMappings(deviceMappings);

    if (template.getIamProfileName().isPresent()) {
      request.withIamInstanceProfile(new IamInstanceProfileSpecification()
          .withName(template.getIamProfileName().get()));
    }

    if (template.getKeyName().isPresent()) {
      request.withKeyName(template.getKeyName().get());
    }

    Placement placement = null;
    if (template.getAvailabilityZone().isPresent()) {
      placement = new Placement().withAvailabilityZone(template.getAvailabilityZone().get());
    }
    if (template.getPlacementGroup().isPresent()) {
      placement = (placement == null) ?
          new Placement().withGroupName(template.getPlacementGroup().get())
          : placement.withGroupName(template.getPlacementGroup().get());
    }
    placement = (placement == null) ?
      new Placement().withTenancy(template.getTenancy())
      : placement.withTenancy(template.getTenancy());

    request.withPlacement(placement);

    return request;
  }

  /**
   * Creates an instance network interface specification based on the specified instance template.
   *
   * @param template the instance template
   * @return instance network interface specification
   */
  private InstanceNetworkInterfaceSpecification getInstanceNetworkInterfaceSpecification(
      EC2InstanceTemplate template) {
    InstanceNetworkInterfaceSpecification network = new InstanceNetworkInterfaceSpecification()
        .withDeviceIndex(0)
        .withSubnetId(template.getSubnetId())
        .withGroups(template.getSecurityGroupIds())
        .withDeleteOnTermination(true)
        .withAssociatePublicIpAddress(associatePublicIpAddresses);

    LOG.info(">> Network interface specification: {}", network);
    return network;
  }

  /**
   * Creates block device mappings based on the specified instance template.
   *
   * @param template the instance template
   * @return the block device mappings
   */
  @SuppressWarnings("ConstantConditions")
  private List<BlockDeviceMapping> getBlockDeviceMappings(EC2InstanceTemplate template) {

    // Query the AMI about the root device name & mapping information
    DescribeImagesResult result = client.describeImages(
        new DescribeImagesRequest().withImageIds(template.getImage()));
    BlockDeviceMapping rootDevice = getFirst(getFirst(result.getImages(), null)
        .getBlockDeviceMappings(), null);

    // The encrypted property was added to the block device mapping in version 1.8 of the SDK.
    // It is a Boolean, but defaults to false instead of being unset, so we set it to null here.
    rootDevice.getEbs().setEncrypted(null);
    rootDevice.getEbs().setVolumeSize(template.getRootVolumeSizeGB());
    rootDevice.getEbs().setVolumeType(template.getRootVolumeType());
    rootDevice.getEbs().setDeleteOnTermination(true);

    List<BlockDeviceMapping> deviceMappings = Lists.newArrayList(rootDevice);

    deviceMappings.addAll(ephemeralDeviceMappings.apply(template.getType()));

    LOG.info(">> Block device mappings: {}", deviceMappings);
    return deviceMappings;
  }

  /**
   * Returns a summary of the specified reservation suitable for logging.
   *
   * @param reservation the reservation
   * @return a summary of the specified reservation suitable for logging
   */
  private String summarizeReservationForLogging(Reservation reservation) {
    StringBuilder builder = new StringBuilder();
    for (Instance instance : reservation.getInstances()) {
      builder.append(String.format("Instance{id=%s privateIp=%s} ",
          instance.getInstanceId(), instance.getPrivateIpAddress()));
    }
    return builder.toString();
  }

  /**
   * <p>Zip two collections as a lazy iterable of pairs.</p>
   * <p><em>Note:</em> the returned iterable is not suitable for repeated use, since it
   * exhausts the iterator over the first collection.</p>
   *
   * @throws IllegalArgumentException if input collections don't have the same size
   */
  private <K, V> Iterable<Map.Entry<K, V>> zipWith(Collection<K> a, Collection<V> b) {
    checkArgument(a.size() == b.size(), "collections don't have the same size");

    final Iterator<K> iterator = a.iterator();
    return Iterables.transform(b, new Function<V, Map.Entry<K, V>>() {
      @Override
      public Map.Entry<K, V> apply(V input) {
        return Maps.immutableEntry(iterator.next(), input);
      }
    });
  }

  /**
   * Waits until all of the specified instances have assigned private IP addresses.
   *
   * @param instanceIds the instance IDs
   * @throws InterruptedException if the operation is interrupted
   */
  private void waitForPrivateIpAddresses(final Collection<String> instanceIds)
      throws InterruptedException {

    final Set<String> instancesWithNoPrivateIpAddress = Sets.newHashSet(instanceIds);

    while (!instancesWithNoPrivateIpAddress.isEmpty()) {
      LOG.info(">> Waiting for {} instance(s) to get a private IP allocated",
          instancesWithNoPrivateIpAddress.size());

      DescribeInstancesResult result = client.describeInstances(
          new DescribeInstancesRequest().withInstanceIds(instancesWithNoPrivateIpAddress));
      forEachInstance(result, new InstanceHandler() {
        @Override
        public void handle(Instance instance) {
          if (instance.getPrivateIpAddress() != null) {
            String ec2InstanceId = instance.getInstanceId();

            LOG.info("<< Instance {} got IP {}", ec2InstanceId, instance.getPrivateIpAddress());

            instancesWithNoPrivateIpAddress.remove(ec2InstanceId);
          }
        }
      });

      if (!instancesWithNoPrivateIpAddress.isEmpty()) {
        LOG.info("Waiting 5 seconds until next check, {} instance(s) still don't have an IP",
            instancesWithNoPrivateIpAddress.size());

        TimeUnit.SECONDS.sleep(5);
      }
    }
  }

  /**
   * Performs a sequence of strict instance ownership checks to avoid any potential harmful
   * accidents.
   *
   * @param instance the instance
   * @param template the template from which the instance was created, or <code>null</code>
   *                 if it is unknown (such as during a delete call)
   * @return the virtual instance ID
   * @throws IllegalStateException if the instance fails an ownership check
   */
  private String checkInstanceIsManagedByDirector(Instance instance, EC2InstanceTemplate template) {
    String virtualInstanceId = getVirtualInstanceId(instance.getTags(), "instance");
    String instanceIds = instance.getInstanceId() + " / " + virtualInstanceId;
    String instanceKeyName = instance.getKeyName();

    if (template != null) {
      if (!template.getKeyName().equals(Optional.fromNullable(instanceKeyName))) {
        throw new IllegalStateException("Found unexpected key name: " + instanceKeyName
            + " for instance: " + instanceIds);
      }
      String instanceType = instance.getInstanceType();
      if (!template.getType().equals(instanceType)) {
        throw new IllegalStateException("Found unexpected type: " + instanceType
            + " for instance: " + instanceIds);
      }
      String instanceImageId = instance.getImageId();
      if (!template.getImage().equals(instanceImageId)) {
        throw new IllegalStateException("Found unexpected image type: " + instanceImageId
            + " for instance: " + instanceIds);
      }
    }
    return virtualInstanceId;
  }

  /**
   * Determines the virtual instance ID from the specified list of tags.
   *
   * @param tags the tags
   * @param type the type of tagged object
   * @return the virtual instance ID
   * @throws IllegalStateException if the tags do not contain the virtual instance ID
   */
  private String getVirtualInstanceId(List<Tag> tags, String type) {
    for (Tag tag : tags) {
      if (tag.getKey().equals(Tags.CLOUDERA_DIRECTOR_ID)) {
        return tag.getValue();
      }
    }

    throw new IllegalStateException(String.format("Any %s managed by " +
        "Cloudera Director should have a %s tag.", type, Tags.CLOUDERA_DIRECTOR_ID));
  }

  /**
   * Returns a map from virtual instance ID to corresponding instance ID for the specified
   * virtual instance IDs.
   *
   * @param virtualInstanceIds the virtual instance IDs
   * @return the map from virtual instance ID to corresponding EC2 instance ID
   */
  private BiMap<String, String> getEC2InstanceIdsByVirtualInstanceId(
      Collection<String> virtualInstanceIds) {
    final BiMap<String, String> ec2InstanceIdsByVirtualInstanceId = HashBiMap.create();
    forEachInstance(virtualInstanceIds, new InstanceHandler() {
      @Override
      public void handle(Instance instance) {
        String virtualInstanceId = checkInstanceIsManagedByDirector(instance, null);
        ec2InstanceIdsByVirtualInstanceId.put(virtualInstanceId, instance.getInstanceId());
      }
    });
    return ec2InstanceIdsByVirtualInstanceId;
  }

  /**
   * Combines all the virtual instance IDs together in a single token than
   * can be used to make sure we can safely retry any runInstances() call.
   *
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Run_Instance_Idempotency.html">Ensuring Idempotency</a>
   */
  private String getHashOfVirtualInstanceIdsForClientToken(Collection<String> virtualInstanceIds,
      Optional<Long> discriminator) {
    // Using MD5 because clientToken should be less than 64 characters long
    Hasher hasher = Hashing.md5().newHasher(virtualInstanceIds.size());

    // We are sorting the input list because we want our hash to be order independent
    for (String id : Sets.newTreeSet(virtualInstanceIds)) {
      hasher.putString(id, Charsets.UTF_8);
    }
    if (discriminator.isPresent()) {
      hasher.putLong(discriminator.get());
    }
    return hasher.hash().toString();
  }

  /**
   * Represents a callback that can be applied to each instance of
   * a {@code DescribeInstancesResult}.
   */
  private interface InstanceHandler {

    /**
     * Handles the specified instance.
     *
     * @param instance the instance
     */
    void handle(Instance instance);
  }

  /**
   * Iterates through the instances identified by the specified virtual instance IDs
   * and calls the specified handler on each instance.
   *
   * @param virtualInstanceIds the virtual instance IDs
   * @param instanceHandler    the instance handler
   */
  private void forEachInstance(Collection<String> virtualInstanceIds,
      EC2Provider.InstanceHandler instanceHandler) {
    DescribeInstancesResult result = client.describeInstances(new DescribeInstancesRequest()
        .withFilters(new Filter().withName("tag:" + Tags.CLOUDERA_DIRECTOR_ID)
            .withValues(virtualInstanceIds)));
    forEachInstance(result, instanceHandler);
  }

  /**
   * Iterates through the instances in the specified {@code DescribeInstancesResult}
   * and calls the specified handler on each instance.
   *
   * @param result          the {@code DescribeInstancesResult}
   * @param instanceHandler the instance handler
   */
  private void forEachInstance(DescribeInstancesResult result, InstanceHandler instanceHandler) {
    List<Reservation> reservations;
    while (!(reservations = result.getReservations()).isEmpty()) {
      for (Reservation reservation : reservations) {
        for (Instance instance : reservation.getInstances()) {
          LOG.debug("Calling instance handler with instance {}", instance);
          instanceHandler.handle(instance);
        }
      }

      if (result.getNextToken() != null) {
        result = client.describeInstances(
            new DescribeInstancesRequest().withNextToken(result.getNextToken()));
      } else {
        break;
      }
    }
  }

  /**
   * Holds details about the allocation state of a single virtual instance.
   */
  @VisibleForTesting
  protected static class SpotAllocationRecord {

    /**
     * The virtual instance ID.
     */
    @VisibleForTesting
    protected final String virtualInstanceId;

    /**
     * The Spot instance request ID, or {@code null} if a Spot instance has not been requested.
     */
    @VisibleForTesting
    protected String spotInstanceRequestId;

    /**
     * The EC2 instance ID, or {@code null} if an instance has not been provisioned.
     */
    @VisibleForTesting
    protected String ec2InstanceId;

    /**
     * Whether the EC2 instance has been tagged.
     */
    @VisibleForTesting
    protected boolean instanceTagged;

    /**
     * The private IP address of the EC2 instance, or {@code null} if the instance does not yet
     * have a private IP address.
     */
    @VisibleForTesting
    protected InetAddress privateIpAddress;

    /**
     * Creates a Spot allocation record with the specified parameters.
     *
     * @param virtualInstanceId the virtual instance ID
     */
    @VisibleForTesting
    protected SpotAllocationRecord(String virtualInstanceId) {
      this.virtualInstanceId = virtualInstanceId;
    }
  }

  /**
   * Holds state and logic for allocating a group of Spot instances. A new instance is
   * required for each allocation request.
   */
  @VisibleForTesting
  protected class SpotGroupAllocator {

    /**
     * The instance template.
     */
    @VisibleForTesting
    protected final EC2InstanceTemplate template;

    /**
     * The virtual instance IDs.
     */
    @VisibleForTesting
    protected final Collection<String> virtualInstanceIds;

    /**
     * The minimum number of instances to allocate if not all resources can be allocated.
     */
    @VisibleForTesting
    protected final int minCount;

    /**
     * The latest time to wait for Spot instance request fulfillment.
     */
    @VisibleForTesting
    protected final Date requestExpirationTime;

    /**
     * The latest time to wait for a Spot price change when it is known that the current Spot price
     * exceeds the Spot bid.
     */
    @VisibleForTesting
    protected final Date priceChangeDeadlineTime;

    /**
     * The map from virtual instance IDs to the corresponding Spot allocation records.
     */
    @VisibleForTesting
    protected final Map<String, SpotAllocationRecord> spotAllocationRecordsByVirtualInstanceId;

    /**
     * The map from untagged Spot instance request IDs to the corresponding Spot instances.
     */
    @VisibleForTesting
    protected final Map<String, String> spotInstancesByUntaggedSpotInstanceRequestId;

    /**
     * The number of allocated instances (including reused orphans).
     */
    @VisibleForTesting
    protected int allocatedInstanceCount = 0;

    /**
     * Creates a Spot group allocator with the specified parameters.
     *
     * @param template              the instance template
     * @param virtualInstanceIds    the virtual instance IDs
     * @param minCount              the minimum number of instances to allocate if not all resources can
     *                              be allocated
     * @param requestExpirationTime the latest time to wait for Spot instance request fulfillment
     * @param priceChangeDeadline   the latest time to wait for a Spot price change when it is known
     *                              that the current Spot price exceeds the Spot bid
     */
    @VisibleForTesting
    protected SpotGroupAllocator(EC2InstanceTemplate template,
        Collection<String> virtualInstanceIds, int minCount, Date requestExpirationTime,
        Date priceChangeDeadline) {
      this.template = template;
      this.virtualInstanceIds = virtualInstanceIds;
      this.minCount = minCount;
      this.requestExpirationTime = requestExpirationTime;
      this.priceChangeDeadlineTime = priceChangeDeadline;
      this.spotAllocationRecordsByVirtualInstanceId =
          initializeSpotAllocationRecordMap(virtualInstanceIds);
      this.spotInstancesByUntaggedSpotInstanceRequestId = Maps.newHashMap();
    }

    /**
     * Initializes the map from virtual instance IDs to the corresponding Spot allocation records.
     *
     * @param virtualInstanceIds the virtual instance IDs
     * @return the map from virtual instance IDs to the corresponding Spot allocation records
     */
    @VisibleForTesting
    protected Map<String, SpotAllocationRecord> initializeSpotAllocationRecordMap(
        Collection<String> virtualInstanceIds) {
      Map<String, SpotAllocationRecord> spotAllocationRecordsByVirtualInstanceId =
          Maps.newLinkedHashMap();
      for (String virtualInstanceId : virtualInstanceIds) {
        SpotAllocationRecord spotAllocationRecord = new SpotAllocationRecord(virtualInstanceId);
        spotAllocationRecordsByVirtualInstanceId.put(virtualInstanceId, spotAllocationRecord);
      }
      return spotAllocationRecordsByVirtualInstanceId;
    }

    /**
     * Returns the Spot allocation record corresponding to the specified virtual instance ID.
     *
     * @param virtualInstanceId the virtual instance ID
     * @return the Spot allocation record corresponding to the specified virtual instance ID
     */
    @VisibleForTesting
    protected SpotAllocationRecord getSpotAllocationRecord(String virtualInstanceId) {
      return spotAllocationRecordsByVirtualInstanceId.get(virtualInstanceId);
    }

    /**
     * Allocates Spot instances.
     *
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    protected void allocate() throws InterruptedException {

      int expectedInstanceCount = virtualInstanceIds.size();

      LOG.info(">> Requesting {} Spot instances for {}", expectedInstanceCount, template);

      boolean success = false;

      PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

      try {
        try {
          // Check for existing instances orphaned by a previous call.
          checkForOrphanedInstances();

          // Check for existing Spot instance requests orphaned by a previous call.
          Set<String> orphanedSpotInstanceRequests = checkForOrphanedSpotInstanceRequests();
          Set<String> pendingRequestIds = Sets.newHashSet(orphanedSpotInstanceRequests);

          // Need to do a Spot instance request for any virtual instance ids not already associated
          // with an orphaned instance or Spot instance request. In the normal use case, this will
          // include all the requested virtual instance ids.
          Set<String> virtualInstanceIdsNeedingSpotInstanceRequest =
              determineVirtualInstanceIdsNeedingSpotInstanceRequest();

          if (!virtualInstanceIdsNeedingSpotInstanceRequest.isEmpty()) {

            // Determine a client token for AWS idempotency. It must take into account the virtual
            // instances being requested, as well as the expiration date of the request, so that we
            // can later make another request for the same virtual instance ids but a different
            // expiration time.
            String clientToken = determineClientToken(virtualInstanceIdsNeedingSpotInstanceRequest);

            // Create a request Spot instances request
            RequestSpotInstancesRequest requestSpotInstancesRequest =
                newRequestSpotInstancesRequest(virtualInstanceIdsNeedingSpotInstanceRequest,
                    clientToken);

            // Request Spot instances
            Set<String> newRequestIds = requestSpotInstances(requestSpotInstancesRequest);

            // Tag Spot instance requests with virtual instance IDs
            tagSpotInstanceRequests(newRequestIds, virtualInstanceIdsNeedingSpotInstanceRequest);

            // Combine the request ids of the reused orphaned requests and the new requests.
            pendingRequestIds.addAll(newRequestIds);

          }

          // Wait for Spot requests to be processed
          waitForSpotInstances(pendingRequestIds, false);

          // Tag all the new instances so that we can easily find them later on.
          tagSpotInstances();

          if (allocatedInstanceCount < minCount) {
            LOG.info(">> Failed to acquire required number of Spot Instances "
                    + "(desired {}, required {}, acquired {})", expectedInstanceCount, minCount,
                allocatedInstanceCount);
          } else {

            // Wait until all of them have a private IP (it should be pretty fast)
            waitForPrivateIpAddresses();

            success = true;
          }
        } finally {
          try {
            cancelSpotRequests(accumulator);
          } finally {
            if (!success) {
              terminateSpotInstances(accumulator);
            }
          }
        }
      } catch (AmazonClientException e) {
        // Log here so we get a full stack trace.
        LOG.error("Problem allocating Spot instances", e);
        throw AWSExceptions.propagate(e);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        // Log here so we get a full stack trace.
        LOG.error("Problem allocating Spot instances", e);
        accumulator.addError(null, getErrorMessage(e));
      }

      if (accumulator.hasError()) {
        PluginExceptionDetails pluginExceptionDetails =
            new PluginExceptionDetails(accumulator.getConditionsByKey());
        throw new UnrecoverableProviderException("Problem allocating Spot instances.",
            pluginExceptionDetails);
      }
    }

    /**
     * <p>Identifies reusable Spot instances orphaned by a previous call.</p>
     * <p><em>Note:</em> because of AWS's eventual consistency policies, we are not guaranteed
     * to be able to detect all orphans here, but we make a best-faith effort.</p>
     *
     * @throws InterruptedException if operation is interrupted
     */
    @VisibleForTesting
    protected void checkForOrphanedInstances()
        throws InterruptedException {

      LOG.info(">> Checking for orphaned Spot instances");
      for (EC2Instance ec2Instance : find(template, virtualInstanceIds)) {
        String ec2InstanceId = ec2Instance.unwrap().getInstanceId();
        String virtualInstanceId = ec2Instance.getId();
        LOG.info(">> Found orphaned instance {} / {}; will reuse", ec2InstanceId, virtualInstanceId);
        SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
        spotAllocationRecord.ec2InstanceId = ec2InstanceId;
        spotAllocationRecord.instanceTagged = true;
        spotAllocationRecord.privateIpAddress = ec2Instance.getPrivateIpAddress();
        allocatedInstanceCount++;
      }
    }

    /**
     * Identifies reusable Spot instance requests orphaned by a previous call.
     *
     * @return the reusable Spot instance requests orphaned by a previous call
     */
    @VisibleForTesting
    protected Set<String> checkForOrphanedSpotInstanceRequests() {

      Set<String> orphanedSpotInstanceRequests = Sets.newHashSet();

      LOG.info(">> Checking for orphaned Spot instance requests");
      DescribeSpotInstanceRequestsRequest describeSpotInstanceRequestsRequest =
          new DescribeSpotInstanceRequestsRequest().withFilters(
              new Filter()
                  .withName("tag:" + Tags.CLOUDERA_DIRECTOR_ID)
                  .withValues(virtualInstanceIds));
      DescribeSpotInstanceRequestsResult describeSpotInstanceRequestsResult =
          client.describeSpotInstanceRequests(describeSpotInstanceRequestsRequest);
      for (SpotInstanceRequest existingSpotInstanceRequest :
          describeSpotInstanceRequestsResult.getSpotInstanceRequests()) {
        String spotInstanceRequestId = existingSpotInstanceRequest.getSpotInstanceRequestId();
        String virtualInstanceId = null;
        for (Tag tag : existingSpotInstanceRequest.getTags()) {
          if (Tags.CLOUDERA_DIRECTOR_ID.equals(tag.getKey())) {
            virtualInstanceId = tag.getValue();
          }
        }
        if (virtualInstanceId == null) {
          LOG.warn(">> Orphaned Spot instance request {} has no virtual instance id",
              spotInstanceRequestId);
        } else {
          SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
          SpotInstanceState spotInstanceState =
              SpotInstanceState.fromValue(existingSpotInstanceRequest.getState());
          switch (spotInstanceState) {
            case Active:
              spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
              String ec2InstanceId = existingSpotInstanceRequest.getInstanceId();
              LOG.info(">> Reusing fulfilled orphaned Spot instance request {} / {} / {}",
                  spotInstanceRequestId, virtualInstanceId, ec2InstanceId);
              if (spotAllocationRecord.ec2InstanceId == null) {
                spotAllocationRecord.ec2InstanceId = ec2InstanceId;
              }
              break;
            case Cancelled:
            case Closed:
            case Failed:
              break;
            default:
              if (existingSpotInstanceRequest.getValidUntil().getTime() > System.currentTimeMillis()) {
                LOG.info(">> Reusing pending orphaned Spot instance request {} / {}",
                    spotInstanceRequestId, virtualInstanceId);
                spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
              }
              break;
          }
        }
      }

      return orphanedSpotInstanceRequests;
    }

    /**
     * Determines which virtual instance IDs require a Spot instance request.
     *
     * @return the virtual instance IDs which require a Spot instance request
     */
    @VisibleForTesting
    @SuppressWarnings("PMD.UselessParentheses")
    protected Set<String> determineVirtualInstanceIdsNeedingSpotInstanceRequest() {

      Set<String> result = Sets.newHashSet();

      LOG.info(">> Determining which virtual instances require Spot instance requests");
      for (Map.Entry<String, SpotAllocationRecord> entry
          : spotAllocationRecordsByVirtualInstanceId.entrySet()) {
        SpotAllocationRecord spotAllocationRecord = entry.getValue();
        if ((spotAllocationRecord.ec2InstanceId == null)
            && (spotAllocationRecord.spotInstanceRequestId == null)) {
          result.add(entry.getKey());
        }
      }

      return result;
    }

    /**
     * Determines the idempotency client token for the specified virtual instance IDs.
     *
     * @param virtualInstanceIds the virtual instance IDs
     * @return the idempotency token
     */
    @VisibleForTesting
    protected String determineClientToken(Set<String> virtualInstanceIds) {
      return getHashOfVirtualInstanceIdsForClientToken(
          virtualInstanceIds, Optional.of(requestExpirationTime.getTime()));
    }

    /**
     * Builds a {@code RequestSpotInstancesRequest} for the specified virtual instance IDs.
     *
     * @param virtualInstanceIds the virtual instance IDs
     * @param clientToken        the idempotency client token
     * @return the {@code RequestSpotInstancesRequest}
     */
    @VisibleForTesting
    protected RequestSpotInstancesRequest newRequestSpotInstancesRequest(
        Collection<String> virtualInstanceIds, String clientToken) {

      LOG.info(">> Building Spot instance requests");
      int groupSize = virtualInstanceIds.size();
      String image = template.getImage();
      String type = template.getType();

      InstanceNetworkInterfaceSpecification network =
          getInstanceNetworkInterfaceSpecification(template);

      List<BlockDeviceMapping> deviceMappings = getBlockDeviceMappings(template);

      LaunchSpecification launchSpecification = new LaunchSpecification()
          .withImageId(image)
          .withInstanceType(type)
          .withNetworkInterfaces(network)
          .withBlockDeviceMappings(deviceMappings);

      if (template.getIamProfileName().isPresent()) {
        launchSpecification.withIamInstanceProfile(new IamInstanceProfileSpecification()
            .withName(template.getIamProfileName().get()));
      }

      if (template.getKeyName().isPresent()) {
        launchSpecification.withKeyName(template.getKeyName().get());
      }

      SpotPlacement placement = null;
      if (template.getAvailabilityZone().isPresent()) {
        placement = new SpotPlacement().withAvailabilityZone(template.getAvailabilityZone().get());
      }
      if (template.getPlacementGroup().isPresent()) {
        placement = (placement == null) ?
            new SpotPlacement().withGroupName(template.getPlacementGroup().get())
            : placement.withGroupName(template.getPlacementGroup().get());
      }
      launchSpecification.withPlacement(placement);

      LOG.info(">> Spot instance request type: {}, image: {}, group size: {}",
        type, image, groupSize);

      return new RequestSpotInstancesRequest()
          .withSpotPrice(template.getSpotBidUSDPerHour().get().toString())
          .withLaunchSpecification(launchSpecification)
          .withInstanceCount(groupSize)
          .withClientToken(clientToken)
          .withValidUntil(requestExpirationTime);
    }

    /**
     * Requests Spot instances, and returns the resulting Spot instance request IDs.
     *
     * @param requestSpotInstancesRequest the {@code RequestSpotInstancesRequest}
     * @return the resulting Spot instance request IDs
     */
    @VisibleForTesting
    protected Set<String> requestSpotInstances(
        RequestSpotInstancesRequest requestSpotInstancesRequest) {
      LOG.info(">> Requesting Spot instances");
      // Call the requestSpotInstances API.
      RequestSpotInstancesResult requestSpotInstancesResult =
          client.requestSpotInstances(requestSpotInstancesRequest);
      List<SpotInstanceRequest> requestResponses =
          requestSpotInstancesResult.getSpotInstanceRequests();

      // Collect the request ids of the reused orphaned requests and the new requests.
      Set<String> newRequestIds = Sets.newHashSet();
      for (SpotInstanceRequest requestResponse : requestResponses) {
        String requestId = requestResponse.getSpotInstanceRequestId();
        LOG.info(">> Created Spot Request {}", requestId);
        newRequestIds.add(requestId);
      }
      return newRequestIds;
    }

    /**
     * Tags the Spot instance requests with the specified IDs with the corresponding virtual
     * instance IDs.
     *
     * @param spotInstanceRequestIds the Spot instance request IDs
     * @param virtualInstanceIds     the corresponding virtual instance IDs
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    protected void tagSpotInstanceRequests(Collection<String> spotInstanceRequestIds,
        Collection<String> virtualInstanceIds) throws InterruptedException {
      List<Tag> userDefinedTags = getUserDefinedTags(template);
      for (Map.Entry<String, String> entry : zipWith(spotInstanceRequestIds, virtualInstanceIds)) {
        String virtualInstanceId = entry.getValue();
        String spotInstanceRequestId = entry.getKey();
        tagSpotInstanceRequest(userDefinedTags, spotInstanceRequestId, virtualInstanceId);
        SpotAllocationRecord spotAllocationRecord = getSpotAllocationRecord(virtualInstanceId);
        spotAllocationRecord.spotInstanceRequestId = spotInstanceRequestId;
      }
    }

    /**
     * Tags an EC2 Spot instance request.
     *
     * @param userDefinedTags       the user-defined tags
     * @param spotInstanceRequestId the Spot instance request ID
     * @param virtualInstanceId     the virtual instance ID
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    @SuppressWarnings("PMD.UselessParentheses")
    protected void tagSpotInstanceRequest(List<Tag> userDefinedTags, String spotInstanceRequestId, String virtualInstanceId)
        throws InterruptedException {
      LOG.info(">> Tagging Spot instance request {} / {}", spotInstanceRequestId, virtualInstanceId);
      List<Tag> tags = Lists.newArrayList(
          new Tag(Tags.CLOUDERA_DIRECTOR_ID, virtualInstanceId),
          new Tag(Tags.CLOUDERA_DIRECTOR_TEMPLATE_NAME, template.getName()));
      tags.addAll(userDefinedTags);

      // Wait for the request to become visible
      while (!spotInstanceRequestExists(spotInstanceRequestId)) {
        TimeUnit.SECONDS.sleep(5);
      }

      // Test failures and google indicate that we can fail to find a request to tag even when we
      // have determined that it exists by describing it. I am adding a retry loop to attempt to
      // avoid this case.
      boolean tagged = false;
      while (!tagged && (System.currentTimeMillis() < requestExpirationTime.getTime())) {
        try {
          client.createTags(
              new CreateTagsRequest().withTags(tags).withResources(spotInstanceRequestId));
          tagged = true;
        } catch (AmazonServiceException e) {
          String errorCode = e.getErrorCode();
          if ("InvalidSpotInstanceRequestID.NotFound".equals(errorCode)) {
            LOG.info(">> Waiting, requestId {}, transient error {}...",
                spotInstanceRequestId, errorCode);
            TimeUnit.SECONDS.sleep(5);
          } else {
            throw e;
          }
        }
      }
    }

    /**
     * Checks whether the specified Spot instance request is visible, <em>i.e.</em> can
     * be located by a describe Spot instance request call.
     *
     * @param spotInstanceRequestId the Spot instance request ID
     * @return whether the Spot instance request is visible, <em>i.e.</em> can
     * be located by a describe Spot instance request call
     */
    @VisibleForTesting
    protected boolean spotInstanceRequestExists(String spotInstanceRequestId) {
      DescribeSpotInstanceRequestsResult result;
      try {
        result = client.describeSpotInstanceRequests(
            new DescribeSpotInstanceRequestsRequest()
                .withSpotInstanceRequestIds(spotInstanceRequestId)
        );
      } catch (AmazonServiceException e) {
        if ("InvalidSpotInstanceRequestID.NotFound".equals(e.getErrorCode())) {
          return false;
        } else {
          throw e;
        }
      }

      for (SpotInstanceRequest spotInstanceRequest : result.getSpotInstanceRequests()) {
        if (spotInstanceRequestId.equals(spotInstanceRequest.getSpotInstanceRequestId())) {
          return true;
        }
      }
      return false;
    }

    /**
     * Waits for pending Spot instance requests to be fulfilled.
     *
     * @param pendingRequestIds the pending Spot instance request IDs
     * @param cancelling        whether we are in the process of cancelling
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    @SuppressWarnings("PMD.EmptyCatchBlock")
    protected void waitForSpotInstances(Set<String> pendingRequestIds, boolean cancelling)
        throws InterruptedException {

      while (!pendingRequestIds.isEmpty()) {
        // Create the describeRequest object with all of the request ids
        // to monitor (e.g. that we started).
        DescribeSpotInstanceRequestsRequest describeRequest =
            new DescribeSpotInstanceRequestsRequest();
        describeRequest.setSpotInstanceRequestIds(pendingRequestIds);

        // Retrieve all of the requests we want to monitor.
        DescribeSpotInstanceRequestsResult describeResult =
            client.describeSpotInstanceRequests(describeRequest);
        List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();

        for (SpotInstanceRequest describeResponse : describeResponses) {
          String requestId = describeResponse.getSpotInstanceRequestId();
          SpotInstanceState spotInstanceState =
              SpotInstanceState.fromValue(describeResponse.getState());
          String statusCodeString = describeResponse.getStatus().getCode();
          SpotInstanceRequestStatusCode statusCode =
              SpotInstanceRequestStatusCode.getSpotInstanceStatusCodeByStatusCodeString(
                  statusCodeString);
          String virtualInstanceId = null;
          try {
            virtualInstanceId =
                getVirtualInstanceId(describeResponse.getTags(), "Spot instance request");
          } catch (IllegalStateException ignore) {
            // Tagging is asynchronous. We may get here before the tagging completes.
          }
          switch (spotInstanceState) {
            case Active:
              if (cancelling) {
                LOG.info(">> Waiting, requestId {}, state {}...", requestId, spotInstanceState);
              } else {
                if (virtualInstanceId == null) {
                  LOG.info(">> Waiting, requestId {} not yet tagged...", requestId);
                } else {
                  pendingRequestIds.remove(requestId);
                  SpotAllocationRecord spotAllocationRecord =
                      getSpotAllocationRecord(virtualInstanceId);
                  if (spotAllocationRecord.ec2InstanceId == null) {
                    spotAllocationRecord.ec2InstanceId = describeResponse.getInstanceId();
                    allocatedInstanceCount++;
                  }
                }
              }
              break;
            case Cancelled:
              pendingRequestIds.remove(requestId);
              switch (statusCode) {
                case REQUEST_CANCELED_AND_INSTANCE_RUNNING:
                  if (virtualInstanceId == null) {
                    String ec2InstanceId = describeResponse.getInstanceId();
                    LOG.info(">> Untagged requestId {} has associated instance {}...", requestId,
                        ec2InstanceId);
                    spotInstancesByUntaggedSpotInstanceRequestId.put(requestId, ec2InstanceId);
                  } else {
                    SpotAllocationRecord spotAllocationRecord =
                        getSpotAllocationRecord(virtualInstanceId);
                    if (spotAllocationRecord.ec2InstanceId == null) {
                      spotAllocationRecord.ec2InstanceId = describeResponse.getInstanceId();
                      allocatedInstanceCount++;
                    }
                  }
                  break;
                default:
                  break;
              }
              break;
            case Closed:
            case Failed:
              pendingRequestIds.remove(requestId);
              break;
            default:
              switch (statusCode) {
                case PRICE_TOO_LOW:
                  if (System.currentTimeMillis() >= priceChangeDeadlineTime.getTime()) {
                    LOG.info("<< Spot bid too low for requestId {}", requestId);
                    pendingRequestIds.remove(requestId);
                  }
                  break;
                default:
                  // Keep looping on Open responses
                  LOG.info(">> Waiting, requestId {}, state {}...", requestId, spotInstanceState);
                  break;
              }
              break;
          }
        }

        if (System.currentTimeMillis() > requestExpirationTime.getTime()) {
          break;
        }

        // TODO add configurable delay
        Thread.sleep(1000);
      }
    }

    /**
     * Tags provisioned Spot instances. Expects that the instances already exists or are in the
     * process of being created.
     *
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    @SuppressWarnings("PMD.UselessParentheses")
    protected void tagSpotInstances() throws InterruptedException {

      List<Tag> userDefinedTags = getUserDefinedTags(template);

      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        if ((spotAllocationRecord.ec2InstanceId != null) && !spotAllocationRecord.instanceTagged) {
          tagInstance(template, userDefinedTags, spotAllocationRecord.virtualInstanceId,
              spotAllocationRecord.ec2InstanceId);
          spotAllocationRecord.instanceTagged = true;
        }
      }
    }

    /**
     * Waits for provisioned Spot instances to have a private IP address.
     *
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    @SuppressWarnings("PMD.UselessParentheses")
    protected void waitForPrivateIpAddresses() throws InterruptedException {
      Set<String> ec2InstanceIds = Sets.newHashSet();
      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
        if ((spotAllocationRecord.privateIpAddress == null) && (ec2InstanceId != null)) {
          ec2InstanceIds.add(ec2InstanceId);
        }
      }

      EC2Provider.this.waitForPrivateIpAddresses(ec2InstanceIds);
    }

    /**
     * Terminates any running Spot instances (includes discovered orphans and allocated instances).
     *
     * @param accumulator the exception condition accumulator
     * @throws InterruptedException if operation is interrupted
     */
    @VisibleForTesting
    protected void terminateSpotInstances(PluginExceptionConditionAccumulator accumulator)
        throws InterruptedException {

      Set<String> ec2InstanceIds = Sets.newHashSet();
      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        String ec2InstanceId = spotAllocationRecord.ec2InstanceId;
        if (ec2InstanceId != null) {
          ec2InstanceIds.add(ec2InstanceId);
        }
      }
      for (String ec2InstanceId : spotInstancesByUntaggedSpotInstanceRequestId.values()) {
        ec2InstanceIds.add(ec2InstanceId);
      }

      if (!ec2InstanceIds.isEmpty()) {
        LOG.info(">> Terminating Spot instances {}", ec2InstanceIds);
        TerminateInstancesResult terminateResult;
        try {
          terminateResult = client.terminateInstances(
              new TerminateInstancesRequest().withInstanceIds(ec2InstanceIds));
          LOG.info("<< Result {}", terminateResult);
        } catch (AmazonClientException e) {
          throw AWSExceptions.propagate(e);
        } catch (Exception e) {
          accumulator.addError(null, "Problem terminating Spot instances: "
              + getErrorMessage(e));
        }
      }
    }

    /**
     * Cancels any outstanding Spot requests (includes discovered orphans and created requests).
     *
     * @param accumulator the exception condition accumulator
     * @throws InterruptedException if the operation is interrupted
     */
    @VisibleForTesting
    protected void cancelSpotRequests(PluginExceptionConditionAccumulator accumulator)
        throws InterruptedException {

      Set<String> spotInstanceRequestIds = Sets.newHashSet();
      for (SpotAllocationRecord spotAllocationRecord :
          spotAllocationRecordsByVirtualInstanceId.values()) {
        String spotInstanceRequestId = spotAllocationRecord.spotInstanceRequestId;
        if (spotInstanceRequestId != null) {
          spotInstanceRequestIds.add(spotInstanceRequestId);
        }
      }

      if (!spotInstanceRequestIds.isEmpty()) {
        LOG.info(">> Canceling Spot instance requests {}", spotInstanceRequestIds);
        CancelSpotInstanceRequestsResult cancelResult;
        try {
          cancelResult = client.cancelSpotInstanceRequests(
              new CancelSpotInstanceRequestsRequest().withSpotInstanceRequestIds(spotInstanceRequestIds));
          LOG.info("<< Result {}", cancelResult);
          waitForSpotInstances(spotInstanceRequestIds, true);
        } catch (AmazonClientException e) {
          throw AWSExceptions.propagate(e);
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          accumulator.addError(null, "Problem canceling Spot instance requests: "
              + getErrorMessage(e));
        }
      }
    }

    /**
     * Returns the error message for the specified exception.
     *
     * @param e the exception
     * @return the error message for the specified exception
     */
    @VisibleForTesting
    protected String getErrorMessage(Exception e) {
      String message = e.getMessage();
      return (message == null) ? e.getClass().getSimpleName() : message;
    }
  }
}

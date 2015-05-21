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

package com.cloudera.director.ec2;

import static com.cloudera.director.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.ASSOCIATE_PUBLIC_IP_ADDRESSES;
import static com.cloudera.director.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION;
import static com.cloudera.director.ec2.EC2Provider.EC2ProviderConfigurationPropertyToken.REGION_ENDPOINT;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PRIVATE_KEY;
import static com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PUBLIC_KEY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Region;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.cloudera.director.InstanceTags;
import com.cloudera.director.Tags;
import com.cloudera.director.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v1.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.Configured;
import com.cloudera.director.spi.v1.model.InstanceState;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.Resource;
import com.cloudera.director.spi.v1.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.model.util.SimpleConfigurationPropertyBuilder;
import com.cloudera.director.spi.v1.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v1.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v1.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v1.util.ConfigurationPropertiesUtil;
import com.cloudera.director.spi.v1.util.KeySerialization;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collection;
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
      .name("EC2")
      .description("AWS EC2 compute provider")
      .providerClass(EC2Provider.class)
      .providerConfigurationProperties(CONFIGURATION_PROPERTIES)
      .resourceTemplateConfigurationProperties(EC2InstanceTemplate.getConfigurationProperties())
      .build();

  /**
   * EC2 configuration properties.
   */
  // Fully qualifying class name due to compiler bug
  public static enum EC2ProviderConfigurationPropertyToken
      implements com.cloudera.director.spi.v1.model.ConfigurationPropertyToken {

    /**
     * Whether to associate a public IP address with instances. Default is <code>true</code>.
     *
     * @see <a href="http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/vpc-ip-addressing.html" />
     */
    ASSOCIATE_PUBLIC_IP_ADDRESSES(new SimpleConfigurationPropertyBuilder()
        .configKey("associatePublicIpAddresses")
        .name("Associate public IP addresses")
        .widget(ConfigurationProperty.Widget.CHECKBOX)
        .defaultValue("true")
        .type(ConfigurationProperty.Type.BOOLEAN)
        .defaultDescription("Whether to associate a public IP address with instances.")
        .build()),

    /**
     * EC2 region. Each region is a separate geographic area. Each region has multiple, isolated
     * locations known as Availability Zones. Default is us-east-1.
     *
     * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html" />
     */
    REGION(new SimpleConfigurationPropertyBuilder()
        .configKey("region")
        .name("EC2 region")
        .defaultValue("us-east-1")
        .defaultDescription("The EC2 region.")
        .widget(ConfigurationProperty.Widget.OPENLIST)
        .addValidValues(
            "ap-northeast-1",
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
     * <p/>
     * <p>This is critical for Gov. cloud because there is no other way to discover those
     * regions.</p>
     */
    REGION_ENDPOINT(new SimpleConfigurationPropertyBuilder()
        .configKey("regionEndpoint")
        .name("EC2 region endpoint")
        .defaultDescription("The EC2 region endpoint.")
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
    private EC2ProviderConfigurationPropertyToken(ConfigurationProperty configurationProperty) {
      this.configurationProperty = configurationProperty;
    }

    @Override
    public ConfigurationProperty unwrap() {
      return configurationProperty;
    }
  }

  private final AmazonEC2Client client;
  private final AmazonIdentityManagementClient identityManagementClient;

  private final EphemeralDeviceMappings ephemeralDeviceMappings;
  private final VirtualizationMappings virtualizationMappings;

  private final boolean associatePublicIpAddresses;

  private final ConfigurationValidator resourceTemplateConfigurationValidator;

  /**
   * Construct a new provider instance and validate all configurations.
   *
   * @param configuration            the configuration
   * @param ephemeralDeviceMappings  the ephemeral device mappings
   * @param virtualizationMappings   the virtualization mappings
   * @param client                   the EC2 client
   * @param identityManagementClient the AIM client
   * @param cloudLocalizationContext the parent cloud localization context
   */
  public EC2Provider(Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      VirtualizationMappings virtualizationMappings, AmazonEC2Client client,
      AmazonIdentityManagementClient identityManagementClient,
      LocalizationContext cloudLocalizationContext) {
    super(configuration, METADATA, cloudLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();
    this.ephemeralDeviceMappings =
        checkNotNull(ephemeralDeviceMappings, "ephemeralDeviceMappings is null");
    this.virtualizationMappings =
        checkNotNull(virtualizationMappings, "virtualizationMappings is null");
    this.client = checkNotNull(client, "client is null");
    this.identityManagementClient = checkNotNull(identityManagementClient,
        "identityManagementClient is null");

    String regionEndpoint = getConfigurationValue(REGION_ENDPOINT, localizationContext);
    if (regionEndpoint != null) {
      LOG.info("<< Using configured region endpoint: {}", regionEndpoint);
    } else {
      String region = getConfigurationValue(REGION, localizationContext);
      regionEndpoint = getEndpointForRegion(this.client, region);
    }
    this.client.setEndpoint(regionEndpoint);

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
    int instanceCount = virtualInstanceIds.size();

    LOG.info(">> Requesting {} instances for {}", instanceCount, template);

    RunInstancesResult runInstancesResult =
        client.runInstances(newRunInstancesRequest(template, virtualInstanceIds, minCount));

    if (!LOG.isInfoEnabled()) {
      LOG.info("<< Reservation {} with {}", runInstancesResult.getReservation().getReservationId(),
          summarizeReservationForLogging(runInstancesResult.getReservation()));
    }

    // Tag all the new instances so that we can easily find them later on

    List<Tag> userDefinedTags = Lists.newArrayListWithExpectedSize(template.getTags().size());
    for (Map.Entry<String, String> entry : template.getTags().entrySet()) {
      userDefinedTags.add(new Tag(entry.getKey(), entry.getValue()));
    }

    final Set<String> instancesWithNoPrivateIp = Sets.newHashSet();

    List<Instance> instances = runInstancesResult.getReservation().getInstances();
    for (Map.Entry<String, Instance> entry : zipWith(virtualInstanceIds, instances)) {

      String virtualInstanceId = entry.getKey();
      Instance instance = entry.getValue();
      String ec2InstanceId = instance.getInstanceId();

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

      if (instance.getPrivateIpAddress() == null) {
        instancesWithNoPrivateIp.add(ec2InstanceId);
      } else {
        LOG.info("<< Instance {} got IP {}", ec2InstanceId, instance.getPrivateIpAddress());
      }
    }

    // Wait until all of them have a private IP (it should be pretty fast)

    while (!instancesWithNoPrivateIp.isEmpty()) {
      LOG.info(">> Waiting for {} instance(s) to get a private IP allocated",
          instancesWithNoPrivateIp.size());

      DescribeInstancesResult result = client.describeInstances(
          new DescribeInstancesRequest().withInstanceIds(instancesWithNoPrivateIp));
      forEachInstance(result, new InstanceHandler() {
        @Override
        public void handle(Instance instance) {
          if (instance.getPrivateIpAddress() != null) {
            String ec2InstanceId = instance.getInstanceId();

            LOG.info("<< Instance {} got IP {}", ec2InstanceId, instance.getPrivateIpAddress());

            instancesWithNoPrivateIp.remove(ec2InstanceId);
          }
        }
      });

      if (!instancesWithNoPrivateIp.isEmpty()) {
        LOG.info("Waiting 5 seconds until next check, {} instance(s) still don't have an IP",
            instancesWithNoPrivateIp.size());

        TimeUnit.SECONDS.sleep(5);
      }
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
      LocalizationContext templatLocalizationContext,
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
        Maps.newHashMap(configuration.getConfiguration(templatLocalizationContext));
    configMap.put(EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME.unwrap().getConfigKey(),
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
   * Returns the endpoint URL for the specified region.
   *
   * @param client     the EC2 client
   * @param regionName the desired region
   * @return the endpoint URL for the specified region
   * @throws IllegalArgumentException if the endpoint cannot be determined
   */
  private String getEndpointForRegion(AmazonEC2Client client, String regionName) {
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
   * Build a {@code RunInstancesRequest} starting from a template and a set of virtual IDs
   *
   * @param template           the instance template
   * @param virtualInstanceIds the virtual instance IDs
   */
  @SuppressWarnings("ConstantConditions")
  private RunInstancesRequest newRunInstancesRequest(EC2InstanceTemplate template,
      Collection<String> virtualInstanceIds, int minCount) {

    int groupSize = virtualInstanceIds.size();

    InstanceNetworkInterfaceSpecification network = new InstanceNetworkInterfaceSpecification()
        .withDeviceIndex(0)
        .withSubnetId(template.getSubnetId())
        .withGroups(template.getSecurityGroupIds())
        .withDeleteOnTermination(true)
        .withAssociatePublicIpAddress(associatePublicIpAddresses);

    LOG.info(">> Network interface specification: {}", network);

    // Query the AMI about the root device name & mapping information
    DescribeImagesResult result = client.describeImages(
        new DescribeImagesRequest().withImageIds(template.getImage()));
    BlockDeviceMapping rootDevice = getFirst(getFirst(result.getImages(), null)
        .getBlockDeviceMappings(), null);

    // set to null as an workaround for a bug in the 1.8.2 version of the AWS SDK
    rootDevice.getEbs().setEncrypted(null);
    rootDevice.getEbs().setVolumeSize(template.getRootVolumeSizeGB());
    rootDevice.getEbs().setVolumeType(template.getRootVolumeType());
    rootDevice.getEbs().setDeleteOnTermination(true);

    List<BlockDeviceMapping> deviceMappings = Lists.newArrayList(rootDevice);

    deviceMappings.addAll(ephemeralDeviceMappings.apply(template.getType()));

    LOG.info(">> Block device mappings: {}", deviceMappings);

    RunInstancesRequest request = new RunInstancesRequest()
        .withImageId(template.getImage())
        .withInstanceType(template.getType())
        .withMaxCount(groupSize)
        .withMinCount(minCount)
        .withClientToken(getHashOfVirtualInstanceIdsForClientToken(virtualInstanceIds))
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
    request.withPlacement(placement);

    return request;
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
   * <p/>
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
    String virtualInstanceId = getVirtualInstanceId(instance.getTags());
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
   * @return the virtual instance ID
   * @throws IllegalStateException if the tags do not contain the virtual instance ID
   */
  private String getVirtualInstanceId(List<Tag> tags) {
    for (Tag tag : tags) {
      if (tag.getKey().equals(Tags.CLOUDERA_DIRECTOR_ID)) {
        return tag.getValue();
      }
    }

    throw new IllegalStateException(String.format("Any instance managed by " +
        "Cloudera Director should have a %s tag.", Tags.CLOUDERA_DIRECTOR_ID));
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
   * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Run_Instance_Idempotency.html" />
   */
  private String getHashOfVirtualInstanceIdsForClientToken(Collection<String> virtualInstanceIds) {
    // Using MD5 because clientToken should be less than 64 characters long
    Hasher hasher = Hashing.md5().newHasher(virtualInstanceIds.size());

    // We are sorting the input list because we want our hash to be order independent
    for (String id : Sets.newTreeSet(virtualInstanceIds)) {
      hasher.putString(id, Charsets.UTF_8);
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
}

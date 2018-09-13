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

package com.cloudera.director.aws.ec2.provider;

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.ASSOCIATE_PUBLIC_IP_ADDRESSES;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.IMPORT_KEY_PAIR_IF_MISSING;
import static com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken.KEY_NAME_PREFIX;
import static com.cloudera.director.aws.ec2.EC2Retryer.retryUntil;
import static com.cloudera.director.aws.ec2.common.EC2Exceptions.INVALID_INSTANCE_ID_NOT_FOUND;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PRIVATE_KEY;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PUBLIC_KEY;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.amazonaws.services.ec2.AmazonEC2AsyncClient;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GetConsoleOutputRequest;
import com.amazonaws.services.ec2.model.GetConsoleOutputResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceAttribute;
import com.amazonaws.services.ec2.model.InstanceAttributeName;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.model.Volume;
import com.amazonaws.services.ec2.model.VolumeState;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.cloudera.director.aws.AWSExceptions;
import com.cloudera.director.aws.AWSFilters;
import com.cloudera.director.aws.AWSTimeouts;
import com.cloudera.director.aws.CustomTagMappings;
import com.cloudera.director.aws.clientprovider.ClientProvider;
import com.cloudera.director.aws.ec2.EC2Instance;
import com.cloudera.director.aws.ec2.EC2InstanceState;
import com.cloudera.director.aws.ec2.EC2InstanceTemplate;
import com.cloudera.director.aws.ec2.EC2ProviderConfigurationPropertyToken;
import com.cloudera.director.aws.ec2.EC2TagHelper;
import com.cloudera.director.aws.ec2.EphemeralDeviceMappings;
import com.cloudera.director.aws.ec2.VirtualizationMappings;
import com.cloudera.director.aws.ec2.allocation.AllocationHelper;
import com.cloudera.director.aws.ec2.allocation.IdType;
import com.cloudera.director.aws.ec2.allocation.InstanceAllocator;
import com.cloudera.director.aws.ec2.allocation.asg.AutoScalingGroupAllocator;
import com.cloudera.director.aws.ec2.allocation.ondemand.OnDemandAllocator;
import com.cloudera.director.aws.ec2.allocation.spot.SpotGroupAllocator;
import com.cloudera.director.aws.ec2.ebs.EBSAllocator;
import com.cloudera.director.aws.ec2.ebs.EBSAllocator.InstanceEbsVolumes;
import com.cloudera.director.aws.ec2.ebs.EBSDeviceMappings;
import com.cloudera.director.aws.ec2.ebs.EBSMetadata;
import com.cloudera.director.aws.ec2.ebs.SystemDisk;
import com.cloudera.director.aws.network.NetworkRules;
import com.cloudera.director.spi.v2.compute.util.AbstractComputeProvider;
import com.cloudera.director.spi.v2.model.ConfigurationProperty;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.InstanceState;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.Resource;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionDetails;
import com.cloudera.director.spi.v2.model.exception.UnrecoverableProviderException;
import com.cloudera.director.spi.v2.model.util.AbstractResource;
import com.cloudera.director.spi.v2.model.util.CompositeConfigurationValidator;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.model.util.SimpleResourceTemplate;
import com.cloudera.director.spi.v2.provider.ResourceProviderMetadata;
import com.cloudera.director.spi.v2.provider.util.SimpleResourceProviderMetadata;
import com.cloudera.director.spi.v2.util.ConfigurationPropertiesUtil;
import com.cloudera.director.spi.v2.util.KeySerialization;
import com.cloudera.director.spi.v2.util.Preconditions;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compute provider of Amazon EC2 instances.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "Guava"})
public class EC2Provider extends AbstractComputeProvider<EC2Instance, EC2InstanceTemplate> {

  private static final Logger LOG = LoggerFactory.getLogger(EC2Provider.class);

  private static final Predicate<Entry<String, Instance>> ENTRY_VALUE_NOT_NULL =
      entry -> entry != null && entry.getValue() != null;

  /**
   * The provider configuration properties.
   */
  private static final List<ConfigurationProperty> CONFIGURATION_PROPERTIES =
      ConfigurationPropertiesUtil.asConfigurationPropertyList(
          EC2ProviderConfigurationPropertyToken.values());

  /**
   * EC2 only allows 95 instance status requests per batch.
   */
  private static final int MAX_INSTANCE_STATUS_REQUESTS = 95;

  /**
   * EC2 only allows 200 tag values to be filtered at a time.
   */
  private static final int MAX_TAG_FILTERING_REQUESTS = 200;

  @VisibleForTesting
  static final String DEVICE_TYPE_EBS = "ebs";

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
   * The default wait time to retrieve host key fingerprints.
   */
  private static final long DEFAULT_WAIT_FOR_HOST_KEY_FINGERPRINTS_MS = 6 * 60 * 1000; // 6 min

  /**
   * Instance allocation strategies.
   */
  private enum InstanceAllocationStrategy {
    ON_DEMAND {
      @Override
      InstanceAllocator allocator(EC2Provider ec2Provider,
          EC2InstanceTemplate template, Collection<String> virtualInstanceIds, int minCount) {

        // Tag EBS volumes if they were part of instance launch request
        boolean tagEbsVolumes = (template != null) &&
            (EBSAllocationStrategy.get(template) == EBSAllocationStrategy.AS_INSTANCE_REQUEST);

        return new OnDemandAllocator(ec2Provider.allocationHelper, ec2Provider.client,
            tagEbsVolumes, ec2Provider.useTagOnCreate,
            template, virtualInstanceIds, minCount);
      }
    },

    SPOT {
      @Override
      InstanceAllocator allocator(EC2Provider ec2Provider,
          EC2InstanceTemplate template, Collection<String> virtualInstanceIds, int minCount) {

        // Tag EBS volumes if they were part of instance launch request
        boolean tagEbsVolumes = (template != null) &&
            (EBSAllocationStrategy.get(template) == EBSAllocationStrategy.AS_INSTANCE_REQUEST);

        return new SpotGroupAllocator(
            ec2Provider.allocationHelper, ec2Provider.client,
            tagEbsVolumes, template, virtualInstanceIds, minCount);
      }
    },

    AUTO_SCALING_GROUP {
      @Override
      InstanceAllocator allocator(EC2Provider ec2Provider,
          EC2InstanceTemplate template, Collection<String> instanceIds, int minCount) {

        return new AutoScalingGroupAllocator(ec2Provider.allocationHelper,
            ec2Provider.client, ec2Provider.autoScalingClient,
            template, instanceIds.size(), minCount);
      }
    };

    /**
     * Returns an instance allocator for the specified parameters.
     *
     * @param ec2Provider the EC2 provider
     * @param template    the instance template
     * @param instanceIds the instance IDs
     * @param minCount    the min count
     * @return the instance allocator
     */
    private static InstanceAllocator getInstanceAllocator(EC2Provider ec2Provider,
        EC2InstanceTemplate template, Collection<String> instanceIds, int minCount) {
      InstanceAllocationStrategy allocationStrategy;
      allocationStrategy = getInstanceAllocationStrategy(template);
      return allocationStrategy.allocator(ec2Provider,
          template, instanceIds, minCount);
    }

    /**
     * Returns the instance allocation strategy for the specified template.
     *
     * @param template the instance template
     * @return the corresponding instance allocation strategy
     */
    private static InstanceAllocationStrategy getInstanceAllocationStrategy(
        EC2InstanceTemplate template) {
      EC2Provider.InstanceAllocationStrategy allocationStrategy;
      if (template.isAutomatic()) {
        allocationStrategy = AUTO_SCALING_GROUP;
      } else if (template.isUseSpotInstances()) {
        allocationStrategy = SPOT;
      } else {
        allocationStrategy = ON_DEMAND;
      }
      return allocationStrategy;
    }

    abstract InstanceAllocator allocator(EC2Provider ec2Provider,
        EC2InstanceTemplate template, Collection<String> instanceIds, int minCount);
  }

  private enum EBSAllocationStrategy {
    NO_EBS_VOLUMES,
    AS_INSTANCE_REQUEST,
    AS_SEPARATE_REQUESTS;

    private static EBSAllocationStrategy get(EC2InstanceTemplate template) {
      if (template.getEbsVolumeCount() == 0 && template.getSystemDisks().size() == 0) {
        return NO_EBS_VOLUMES;
      }

      // Ideally we want to request EBS volumes as part of the instance launch request.
      // However due to AWS API limitations, requesting encrypted EBS volumes with a
      // user specified KMS key can not be done as part of instance launch for certain
      // regions (like GovCloud). In this scenario we allow the option to individually
      // create and attach each EBS volume separately after instance launch.

      if (requiresSeparateEbsAllocation(template)) {
        return AS_SEPARATE_REQUESTS;
      } else {
        return AS_INSTANCE_REQUEST;
      }
    }
  }

  private static boolean requiresSeparateEbsAllocation(EC2InstanceTemplate template) {
    return template.isAllocateEbsSeparately() && hasVolumesWithKmsKey(template);
  }

  private static boolean hasVolumesWithKmsKey(EC2InstanceTemplate template) {
    boolean hasVolumesWithKmsKey = (template.getEbsVolumeCount() != 0) ?
        template.getEbsKmsKeyId().isPresent() :
        template.getSystemDisks().get(0).getKmsKeyId() != null;

    for (SystemDisk systemDisk : template.getSystemDisks()) {
      boolean systemDiskUsingKmsKey = systemDisk.getKmsKeyId() != null;
      if (hasVolumesWithKmsKey && !systemDiskUsingKmsKey) {
        throw new IllegalArgumentException(
            "Either all volumes should be encrypted with KMS Key ID or none"
        );
      }
    }
    return hasVolumesWithKmsKey;
  }

  private final AmazonEC2AsyncClient client;
  private final AmazonIdentityManagementClient identityManagementClient;
  private final AWSKMSClient kmsClient;
  private final AmazonAutoScalingAsyncClient autoScalingClient;

  private final EphemeralDeviceMappings ephemeralDeviceMappings;
  private final EBSDeviceMappings ebsDeviceMappings;
  private final VirtualizationMappings virtualizationMappings;
  private final AWSFilters ec2Filters;
  private final NetworkRules networkRules;

  @VisibleForTesting
  final EC2TagHelper ec2TagHelper;

  @VisibleForTesting
  final AllocationHelperImpl allocationHelper;

  private final boolean associatePublicIpAddresses;
  private final boolean importKeyPairIfMissing;
  private final String keyNamePrefix;

  private final ConfigurationValidator resourceTemplateConfigurationValidator;

  private final ConsoleOutputExtractor consoleOutputExtractor;

  private final AWSTimeouts awsTimeouts;

  private final boolean useTagOnCreate;

  /**
   * Construct a new provider instance and validate all configurations.
   *
   * @param configuration                    the configuration
   * @param ephemeralDeviceMappings          the ephemeral device mappings
   * @param ebsDeviceMappings                the ebs device mappings
   * @param ebsMetadata                      the EBS metadata
   * @param virtualizationMappings           the virtualization mappings
   * @param awsFilters                       the AWS filters
   * @param awsTimeouts                      the AWS timeouts
   * @param customTagMappings                the custom tag mappings
   * @param networkRules                     the network rules
   * @param clientProvider                   the EC2 client provider
   * @param identityManagementClientProvider the IAM client provider
   * @param kmsClientProvider                the KMS client provider
   * @param cloudLocalizationContext         the parent cloud localization context
   * @throws UnrecoverableProviderException if an unrecoverable exception occurs communicating with
   *                                        the provider
   */
  public EC2Provider(
      Configured configuration,
      EphemeralDeviceMappings ephemeralDeviceMappings,
      EBSDeviceMappings ebsDeviceMappings,
      EBSMetadata ebsMetadata,
      VirtualizationMappings virtualizationMappings,
      AWSFilters awsFilters,
      AWSTimeouts awsTimeouts,
      CustomTagMappings customTagMappings,
      NetworkRules networkRules,
      ClientProvider<AmazonEC2AsyncClient> clientProvider,
      ClientProvider<AmazonAutoScalingAsyncClient> autoScalingClientProvider,
      ClientProvider<AmazonIdentityManagementClient> identityManagementClientProvider,
      ClientProvider<AWSKMSClient> kmsClientProvider,
      boolean useTagOnCreate,
      LocalizationContext cloudLocalizationContext) {

    super(configuration, METADATA, cloudLocalizationContext);
    LocalizationContext localizationContext = getLocalizationContext();
    this.ephemeralDeviceMappings =
        requireNonNull(ephemeralDeviceMappings, "ephemeralDeviceMappings is null");
    this.ebsDeviceMappings =
        requireNonNull(ebsDeviceMappings, "ebsDeviceMappings is null");
    this.virtualizationMappings =
        requireNonNull(virtualizationMappings, "virtualizationMappings is null");
    this.ec2Filters = requireNonNull(awsFilters, "awsFilters").getSubfilters(ID);
    this.ec2TagHelper = new EC2TagHelper(customTagMappings);
    this.networkRules = requireNonNull(networkRules, "networkRules is null");

    PluginExceptionConditionAccumulator accumulator = new PluginExceptionConditionAccumulator();

    this.client = requireNonNull(clientProvider, "clientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);
    this.autoScalingClient = requireNonNull(
        autoScalingClientProvider, "autoScalingClientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);
    this.identityManagementClient = requireNonNull(
        identityManagementClientProvider, "identityManagementClientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);
    this.kmsClient = requireNonNull(kmsClientProvider, "kmsClientProvider is null")
        .getClient(configuration, accumulator, localizationContext, false);

    if (accumulator.hasError()) {
      PluginExceptionDetails pluginExceptionDetails =
          new PluginExceptionDetails(accumulator.getConditionsByKey());
      throw new UnrecoverableProviderException("Provider initialization failed",
          pluginExceptionDetails);
    }

    this.associatePublicIpAddresses = Boolean.parseBoolean(
        getConfigurationValue(ASSOCIATE_PUBLIC_IP_ADDRESSES, localizationContext));

    this.importKeyPairIfMissing = Boolean.parseBoolean(
        getConfigurationValue(IMPORT_KEY_PAIR_IF_MISSING, localizationContext));
    this.keyNamePrefix = getConfigurationValue(KEY_NAME_PREFIX, localizationContext);

    this.awsTimeouts = awsTimeouts;

    this.allocationHelper = new AllocationHelperImpl();

    this.resourceTemplateConfigurationValidator =
        new CompositeConfigurationValidator(
            METADATA.getResourceTemplateConfigurationValidator(),
            new EC2InstanceTemplateConfigurationValidator(this, ebsMetadata),
            new EC2NetworkValidator(this)
        );

    this.consoleOutputExtractor = new ConsoleOutputExtractor();

    this.useTagOnCreate = useTagOnCreate;
  }

  /**
   * Returns the AWS EC2 client.
   *
   * @return the AWS EC2 client
   */
  public AmazonEC2Client getClient() {
    return client;
  }

  /**
   * Returns the AWS auto scaling client.
   *
   * @return the AWS auto scaling client
   */
  public AmazonAutoScalingAsyncClient getAutoScalingClient() {
    return autoScalingClient;
  }

  /**
   * Returns the AWS identity management client.
   *
   * @return the AWS identity management client
   */
  @SuppressWarnings("WeakerAccess")
  public AmazonIdentityManagementClient getIdentityManagementClient() {
    return identityManagementClient;
  }

  /**
   * Returns the AWS KMS client.
   *
   * @return the AWS KMS client
   */
  @SuppressWarnings("WeakerAccess")
  public AWSKMSClient getKmsClient() {
    return kmsClient;
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
  @SuppressWarnings("WeakerAccess")
  public AWSFilters getEC2Filters() {
    return ec2Filters;
  }

  /**
   * Returns the EC2 tag helper.
   *
   * @return the EC2 tag helper
   */
  @SuppressWarnings("WeakerAccess")
  public EC2TagHelper getEC2TagHelper() {
    return ec2TagHelper;
  }

  /**
   * Returns the network rules.
   *
   * @return the network rules
   */
  @SuppressWarnings("WeakerAccess")
  public NetworkRules getNetworkRules() {
    return networkRules;
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
  public Collection<EC2Instance> allocate(
      EC2InstanceTemplate template,
      Collection<String> instanceIds,
      int minCount)
      throws InterruptedException {

    // TODO: This should really be taken care of in the SPI validate command.
    // We are punting this for a future change that compels us to release a new version of SPI
    ec2TagHelper.validateTags(template.getTags());

    InstanceAllocator allocator = InstanceAllocationStrategy.getInstanceAllocator(
        this, template, instanceIds, minCount);

    Collection<EC2Instance> allocatedInstances = allocator.allocate();

    if (EBSAllocationStrategy.get(template) == EBSAllocationStrategy.AS_SEPARATE_REQUESTS) {
      if (allocatedInstances.size() > 0) {
        LOG.info(">> Allocating EBS volumes");
        Map<String, String> instanceIdPairs = allocatedInstances.stream().collect(
            Collectors.toMap(AbstractResource::getId, (i) -> i.unwrap().getInstanceId()));

        allocateEbsVolumes(template, instanceIdPairs, minCount);
      } else {
        LOG.info(">> Skipping EBS volume allocation since no instances were allocated");
      }
    }

    return allocatedInstances;
  }

  @Override
  @SuppressWarnings({"PMD.UnusedFormalParameter"})
  public void delete(EC2InstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException {

    InstanceAllocationStrategy instanceAllocationStrategy =
        (template == null)
            ? InstanceAllocationStrategy.ON_DEMAND
            : InstanceAllocationStrategy.getInstanceAllocationStrategy(template);
    InstanceAllocator allocator =
        instanceAllocationStrategy.allocator(this, template, instanceIds, 0);
    allocator.delete();
  }

  @Override
  public Map<String, Set<String>> getHostKeyFingerprints(EC2InstanceTemplate template,
      Collection<String> instanceIds)
      throws InterruptedException {

    Map<String, String> idsToCheck = isAutomatic(template)
        ? instanceIds.stream().collect(Collectors.toMap((id) -> id, (id) -> id))
        : Maps.newHashMap(getEC2InstanceIdsByVirtualInstanceId(instanceIds));

    Map<String, Set<String>> hostKeyFingerprints = Maps.newHashMapWithExpectedSize(idsToCheck.size());

    LOG.info("Waiting for EC2 console output to display its host key fingerprint for instance(s): {}",
        idsToCheck.keySet());

    Stopwatch stopwatch = Stopwatch.createStarted();
    do {
      Iterator<Entry<String, String>> it = idsToCheck.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, String> entry = it.next();
        String instanceId = entry.getKey();
        String ec2InstanceId = entry.getValue();

        GetConsoleOutputRequest consoleOutputRequest = new GetConsoleOutputRequest()
            .withInstanceId(ec2InstanceId);
        GetConsoleOutputResult result = client.getConsoleOutput(consoleOutputRequest);

        if (result.getOutput() != null) {
          String consoleOutput = result.getDecodedOutput();
          if (!consoleOutputExtractor.hasHostKeyFingerprintBlock(consoleOutput)) {
            LOG.debug("EC2 Console Output doesn't contain the host key fingerprint yet, retrying ...");
            continue;
          }

          Set<String> instanceHostKeyFingerprints
              = consoleOutputExtractor.getHostKeyFingerprints(result.getDecodedOutput());

          LOG.debug("Host key fingerprints for ID {} are {}", instanceId, instanceHostKeyFingerprints);
          hostKeyFingerprints.put(instanceId, instanceHostKeyFingerprints);
          it.remove();
        } else {
          LOG.debug("EC2 Console Output empty, retrying soon ...");
        }
      }

      if (idsToCheck.isEmpty()) {
        break;
      }

      TimeUnit.SECONDS.sleep(10);
    } while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < DEFAULT_WAIT_FOR_HOST_KEY_FINGERPRINTS_MS);

    if (!idsToCheck.isEmpty()) {
      LOG.warn("Couldn't retrieve SSH host key fingerprints for following {} instance(s): {}",
          idsToCheck.size(), idsToCheck.keySet());
    }

    return hostKeyFingerprints;
  }

  /**
   * Creates and attaches EBS volumes to EC2 instances. This expects that the instances
   * have already been allocated. Instances that could not acquire the correct number of
   * EBS volumes will be terminated along with any leftover volumes. If the minimum
   * number of instances could not acquire EBS volumes, all the specified instances and
   * leftover volumes will be terminated.
   *
   * @param template        the EC2 instance template that contains EBS configurations
   * @param instanceIdPairs map from instance IDs to EC2 instance IDs to attach volumes to
   * @param minCount        the minimum number of instances that need EBS volumes attached
   * @throws InterruptedException if the process is interrupted
   */
  private void allocateEbsVolumes(EC2InstanceTemplate template, Map<String, String> instanceIdPairs,
      int minCount)
      throws InterruptedException {

    boolean success = false;

    Image image = getImage(template.getImage());
    Set<String> existingDeviceNames = getExistingDeviceNames(image.getBlockDeviceMappings());

    EBSAllocator ebsAllocator = new EBSAllocator(this.client, this.awsTimeouts, ec2TagHelper, ebsDeviceMappings,
        existingDeviceNames, useTagOnCreate);

    List<InstanceEbsVolumes> instanceVolumes = ebsAllocator.createVolumes(template, instanceIdPairs);

    try {
      try {
        instanceVolumes = ebsAllocator.waitUntilVolumesAvailable(instanceVolumes);
        instanceVolumes = ebsAllocator.attachAndOptionallyTagVolumes(template, instanceVolumes);
      } finally {
        // Ensure that delete on termination is set for attached instances no matter what. It's possible
        // for the EBS volume/ attachment to time out but still succeed, which can lead to leaked EBS
        // volumes. This should ensure that the instances are not leaked accidentally.
        instanceVolumes = ebsAllocator.getUpdatedVolumeInfo(instanceVolumes); // Update volume states
        ebsAllocator.addDeleteOnTerminationFlag(instanceVolumes);
      }

      int successfulInstances = getSuccessfulInstanceCount(instanceVolumes);
      LOG.info("{} out of {} instances successfully acquired EBS volumes",
          successfulInstances, instanceIdPairs.size());

      if (successfulInstances < minCount) {
        LOG.warn("A minimum number of {} instances could not acquire EBS volumes, cleaning up by " +
            "deleting all allocated instances and volumes", minCount);
      } else {
        success = true;
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new UnrecoverableProviderException("Unexpected problem allocating EBS volumes", e);
    } finally {
      if (!success) {
        deleteAllInstancesAndVolumes(instanceVolumes, template, ebsAllocator);
      } else {
        deleteFailedInstancesAndVolumes(instanceVolumes, template, ebsAllocator);
      }
    }
  }

  /**
   * Terminates failed instances and their associated volumes. In this case a failed
   * instance is any instance that doesn't have all their volumes as ATTACHED.
   */
  private void deleteFailedInstancesAndVolumes(List<InstanceEbsVolumes> instanceEbsVolumesList,
      EC2InstanceTemplate template, EBSAllocator ebsAllocator) throws InterruptedException {

    Set<String> instancesToTerminate = Sets.newHashSet();

    nextInstanceVolume:
    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      for (VolumeState status : instanceEbsVolumes.getVolumeStates().values()) {
        if (status != VolumeState.InUse) {
          deleteCreatedVolumes(instanceEbsVolumes, ebsAllocator);
          instancesToTerminate.add(instanceEbsVolumes.getInstanceId());
          continue nextInstanceVolume;
        }
      }
    }
    delete(template, instancesToTerminate);
  }


  /**
   * Terminates a list of instances and deletes their associated volumes.
   */
  private void deleteAllInstancesAndVolumes(List<InstanceEbsVolumes> instanceEbsVolumesList,
      EC2InstanceTemplate template, EBSAllocator ebsAllocator) throws InterruptedException {

    Set<String> instancesToTerminate = Sets.newHashSet();

    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      deleteCreatedVolumes(instanceEbsVolumes, ebsAllocator);
      instancesToTerminate.add(instanceEbsVolumes.getInstanceId());
    }
    delete(template, instancesToTerminate);
  }

  private void deleteCreatedVolumes(InstanceEbsVolumes instanceEbsVolumes, EBSAllocator ebsAllocator)
      throws InterruptedException {
    Map<String, VolumeState> volumesToDelete = Maps.newHashMap();
    for (Entry<String, VolumeState> volumeIdAndStatus
        : instanceEbsVolumes.getVolumeStates().entrySet()) {
      String volumeId = volumeIdAndStatus.getKey();
      VolumeState state = volumeIdAndStatus.getValue();

      if (!volumeId.startsWith(InstanceEbsVolumes.UNCREATED_VOLUME_ID)) {
        volumesToDelete.put(volumeId, state);
      }
    }
    ebsAllocator.deleteVolumes(volumesToDelete);
  }

  /**
   * Get the number of instances that have successfully acquired volumes. Success means
   * that all volumes associated with an instance are in the ATTACHED state.
   */
  private int getSuccessfulInstanceCount(List<InstanceEbsVolumes> instanceEbsVolumesList) {
    int count = 0;
    for (InstanceEbsVolumes instanceEbsVolumes : instanceEbsVolumesList) {
      boolean success = true;
      for (VolumeState state : instanceEbsVolumes.getVolumeStates().values()) {
        if (state != VolumeState.InUse) {
          success = false;
          break;
        }
      }
      if (success) count++;
    }
    return count;
  }

  /**
   * Get EBS volumes attached to the specified virtual instance id.
   *
   * @return list of ebs volumes
   */
  @VisibleForTesting
  List<Volume> getVolumes(String virtualInstanceId) {
    String ec2InstanceId = getOnlyElement(
        getEC2InstanceIdsByVirtualInstanceId(
            Collections.singletonList(virtualInstanceId)
        ).values()
    );

    InstanceAttribute instanceAttribute =
        describeInstanceAttribute(ec2InstanceId, InstanceAttributeName.BlockDeviceMapping);
    List<InstanceBlockDeviceMapping> blockDeviceMappings =
        instanceAttribute.getBlockDeviceMappings();

    List<String> volumeIds = Lists.newArrayList();
    for (InstanceBlockDeviceMapping mapping : blockDeviceMappings) {
      volumeIds.add(mapping.getEbs().getVolumeId());
    }

    DescribeVolumesResult volumeResults = client.describeVolumes(
        new DescribeVolumesRequest().withVolumeIds(volumeIds)
    );

    return volumeResults.getVolumes();
  }

  @Override
  public Collection<EC2Instance> find(final EC2InstanceTemplate template, Collection<String> instanceIds)
      throws InterruptedException {
    LOG.debug("Finding instances {}", instanceIds);

    Collection<EC2Instance> ec2Instances = FluentIterable
        .from(allocationHelper.doFind(template, instanceIds))
        .transform(idToInstance -> {
          requireNonNull(idToInstance, "idToInstance is null");
          Instance instance = idToInstance.getValue();
          fillMissingProperties(instance);
          return new EC2Instance(template, idToInstance.getKey(), instance);
        })
        .toList();

    LOG.debug("Found {} instances for {} instance IDs", ec2Instances.size(), instanceIds.size());
    return ec2Instances;
  }


  private void fillMissingProperties(Instance instance) {
    try {
      InstanceAttribute instanceAttribute = describeInstanceAttribute(
          instance.getInstanceId(), InstanceAttributeName.SriovNetSupport);
      String sriovNetSupport = instanceAttribute.getSriovNetSupport();
      instance.setSriovNetSupport(sriovNetSupport);
    } catch (AmazonServiceException e) {
      // In practice, users may not have appropriate IAM permission for
      // DescribeInstanceAttribute. We need to be more forgiving in those cases,
      // and simply leave a warning in the log here.
      LOG.warn("Could not fill missing properties. Failed to perform " +
          "DescribeInstanceAttribute action.", e);
    }
  }

  /**
   * Retrieves an instance attribute.
   *
   * @param instanceId instance ID
   * @param attribute  name of attribute to describe
   * @return attribute
   */
  @VisibleForTesting
  InstanceAttribute describeInstanceAttribute(String instanceId, InstanceAttributeName attribute) {
    DescribeInstanceAttributeRequest request = new DescribeInstanceAttributeRequest()
        .withInstanceId(instanceId)
        .withAttribute(attribute);

    return client.describeInstanceAttribute(request).getInstanceAttribute();
  }

  @Override
  public Map<String, InstanceState> getInstanceState(EC2InstanceTemplate template,
      Collection<String> instanceIds) {
    IdType idType = getIdType(template);

    Map<String, InstanceState> instanceStateByInstanceId =
        Maps.newHashMapWithExpectedSize(instanceIds.size());

    // Partition full requests into multiple batch requests, AWS limits
    // the total number of instance status requests you can make.
    List<List<String>> partitions =
        Lists.partition(Lists.newArrayList(instanceIds), MAX_INSTANCE_STATUS_REQUESTS);

    for (List<String> partition : partitions) {
      instanceStateByInstanceId.putAll(getBatchInstanceState(partition, idType));
    }

    return instanceStateByInstanceId;
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
      if (importKeyPairIfMissing) {
        keyName = keyNamePrefix + publicKeyFingerprint;
        LOG.info("KeyPair not found. Adding public key to EC2 with key name : {}", keyName);
        try {
          //noinspection ConstantConditions
          client.importKeyPair(new ImportKeyPairRequest().withKeyName(keyName).withPublicKeyMaterial(
              BaseEncoding.base64().encode(publicKey.getEncoded())));
        } catch (AmazonEC2Exception e) {
          if (e.getErrorCode().equals("UnauthorizedOperation")) {
            String message = String.format("No private key in EC2 matches the " +
                "fingerprint %s. To auto register keys, " +
                "add ec2:ImportKeyPair permission", privateKeyFingerprint);
            throw new AmazonEC2Exception(message);
          }
          throw e;
        }
      } else {
        throw new IllegalArgumentException("No private key in EC2 matches the fingerprint " +
            privateKeyFingerprint);
      }
    } else {
      LOG.info("Found EC2 key name {} for fingerprint", keyName);
    }
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
   * @return private key fingerprint, as a lowercase hex string with colons
   * @throws IllegalStateException if the SHA-1 digest algorithm is not
   *                               available
   */
  @VisibleForTesting
  static String getSha1Fingerprint(PrivateKey privateKey) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    return toColonSeparatedHexString(digest, privateKey.getEncoded());
  }

  /**
   * Gets the MD5 digest of the public key bits. This is used as a fingerprint
   * by AWS for an imported key pair.
   *
   * @param publicKey public key
   * @return public key fingerprint, as a lowercase hex string with colons,
   * or null if publicKey is null
   */
  @VisibleForTesting
  static String getMd5Fingerprint(PublicKey publicKey) {
    if (publicKey == null) {
      return null;
    }
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    return toColonSeparatedHexString(digest, publicKey.getEncoded());
  }

  private static String toColonSeparatedHexString(MessageDigest digest, byte[] key) {
    byte[] fingerprintBytes = digest.digest(key);
    String binaryHexString =
        DatatypeConverter.printHexBinary(fingerprintBytes).toLowerCase(Locale.US);
    return toColonSeparatedString(binaryHexString);
  }

  @VisibleForTesting
  static String toColonSeparatedString(String string) {
    // Matches every pair of characters (excluding the pair at in the end) and
    // inserts a colon between them
    return string.replaceAll("..(?!$)", "$0:");
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
    DescribeKeyPairsRequest request = new DescribeKeyPairsRequest()
        .withFilters(new Filter()
            .withName("fingerprint")
            .withValues(privateKeyFingerprint, publicKeyFingerprint)
        );
    DescribeKeyPairsResult result = client.describeKeyPairs(request);
    if (result.getKeyPairs().size() > 0) {
      return result.getKeyPairs().get(0).getKeyName();
    }
    return null;
  }

  /**
   * Returns a map from instance IDs to instance state for the specified batch of instance IDs.
   *
   * @param instanceIds batch of instance IDs
   * @return the map from instance IDs to instance state for the specified batch of instance IDs
   */
  private Map<String, InstanceState> getBatchInstanceState(Collection<String> instanceIds, IdType idType) {
    final Map<String, InstanceState> instanceStateByInstanceId =
        Maps.newHashMapWithExpectedSize(instanceIds.size());

    allocationHelper.forEachInstance(instanceIds, instance -> {
      Preconditions.checkNotNull(instance, "instance is null");
      instanceStateByInstanceId.put(
          getInstanceId(null, instance, idType),
          EC2InstanceState.fromInstanceStateName(
              InstanceStateName.fromValue(instance.getState().getName())));
      return null;
    }, idType);

    return instanceStateByInstanceId;
  }

  private Image getImage(String imageId) {
    DescribeImagesResult result = client.describeImages(
        new DescribeImagesRequest().withImageIds(imageId));
    if (result.getImages().isEmpty()) {
      throw new IllegalArgumentException("The description for image " + imageId +
          " is empty");
    }
    return result.getImages().get(0);
  }

  private Set<String> getExistingDeviceNames(List<BlockDeviceMapping> mappings) {
    Set<String> existingDeviceNames = Sets.newHashSet();
    for (BlockDeviceMapping mapping : mappings) {
      existingDeviceNames.add(mapping.getDeviceName());
    }
    return existingDeviceNames;
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
    String virtualInstanceId = allocationHelper.getVirtualInstanceId(instance.getTags(), "instance");
    String instanceIds = instance.getInstanceId() + " / " + virtualInstanceId;
    String instanceKeyName = instance.getKeyName();

    if (template != null) {
      if (!template.getKeyName().equals(Optional.fromNullable(instanceKeyName))) {
        LOG.warn("Found unexpected key name: {} for instance: {}", instanceKeyName, instanceIds);
      }
      String instanceType = instance.getInstanceType();
      if (!template.getType().equals(instanceType)) {
        LOG.warn("Found unexpected type: {} for instance: {}", instanceType, instanceIds);
      }
      String instanceImageId = instance.getImageId();
      if (!template.getImage().equals(instanceImageId)) {
        LOG.warn("Found unexpected image: {} for instance: {}", instanceImageId, instanceIds);
      }
    }
    return virtualInstanceId;
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
    allocationHelper.forEachInstance(virtualInstanceIds, instance -> {
      Preconditions.checkNotNull(instance, "instance is null");
      String virtualInstanceId = checkInstanceIsManagedByDirector(instance, null);
      ec2InstanceIdsByVirtualInstanceId.put(virtualInstanceId, instance.getInstanceId());
      return null;
    }, IdType.VIRTUAL_INSTANCE_ID);
    return ec2InstanceIdsByVirtualInstanceId;
  }

  /**
   * Selects the root device from a list of block device mappings based on the
   * root device name for the mappings' image.
   *
   * @param mappings       list of block device mappings
   * @param rootDeviceName image root device name
   * @return root device mapping, or null if it could not be determined
   */
  @VisibleForTesting
  static BlockDeviceMapping selectRootDevice(List<BlockDeviceMapping> mappings, String rootDeviceName) {
    /*
     * Heuristic to find the root device:
     * - The best match is the EBS device that matches the root device name for the image, but
     *   this may not happen (/dev/sda1 vs. /dev/sda). See:
     *   http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
     * - If there isn't a best match, then a device whose name is a prefix for the image's root
     *   device name is selected.
     * - If all else fails, it's the first EBS volume in the list.
     */
    BlockDeviceMapping bestMatch = null;
    BlockDeviceMapping firstEbs = null;
    for (BlockDeviceMapping mapping : mappings) {
      if (mapping.getEbs() == null) {
        continue;
      }
      if (firstEbs == null) {
        firstEbs = mapping;
      }
      if (mapping.getDeviceName() == null) {
        continue;
      }
      if (rootDeviceName.equals(mapping.getDeviceName())) {
        return mapping;
      }
      if (rootDeviceName.startsWith(mapping.getDeviceName())) {
        bestMatch = mapping;
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    } else if (firstEbs != null) {
      return firstEbs;
    } else {
      return null;
    }
  }

  /**
   * Returns the ID type for the specified template.
   *
   * @param template the instance template
   * @return the ID type
   */
  private IdType getIdType(EC2InstanceTemplate template) {
    return isAutomatic(template) ? IdType.EC2_INSTANCE_ID : IdType.VIRTUAL_INSTANCE_ID;
  }

  /**
   * Returns whether the specified instance template represents an automatic instance group.
   *
   * @param template the instance template
   * @return whether the specified instance template represents an automatic instance group
   */
  private boolean isAutomatic(EC2InstanceTemplate template) {
    return (template != null) && template.isAutomatic();
  }

  /**
   * Returns the instance ID of the specified type for the specified instance.
   *
   * @param template the instance template
   * @param instance the instance
   * @param idType   the ID type
   * @return the instance ID of the specified type for the specified instance
   */
  private String getInstanceId(EC2InstanceTemplate template, Instance instance, IdType idType) {
    String instanceId;
    switch (idType) {
      case VIRTUAL_INSTANCE_ID:
        instanceId = checkInstanceIsManagedByDirector(instance, template);
        break;
      case EC2_INSTANCE_ID:
        instanceId = instance.getInstanceId();
        break;
      default:
        throw new IllegalStateException("Unknown instance ID type.");
    }
    return instanceId;
  }

  @VisibleForTesting
  class AllocationHelperImpl implements AllocationHelper {

    @Override
    public AWSTimeouts getAWSTimeouts() {
      return awsTimeouts;
    }

    @Override
    public EC2TagHelper getEC2TagHelper() {
      return ec2TagHelper;
    }

    @Override
    public boolean waitUntilInstanceHasStarted(String ec2InstanceId, DateTime timeout) throws InterruptedException, TimeoutException {

      Callable<Boolean> task = () -> {
        DescribeInstanceStatusResult result = client.describeInstanceStatus(
            new DescribeInstanceStatusRequest()
                .withIncludeAllInstances(true)
                .withInstanceIds(ec2InstanceId)
        );
        for (InstanceStatus status : result.getInstanceStatuses()) {
          if (ec2InstanceId.equals(status.getInstanceId())) {
            InstanceStateName currentState = InstanceStateName.fromValue(status.getInstanceState().getName());

            if (currentState.equals(InstanceStateName.Terminated) ||
                currentState.equals(InstanceStateName.ShuttingDown)) {
              if (LOG.isErrorEnabled()) {
                LOG.error("Instance {} has unexpectedly terminated", ec2InstanceId);
              }
              return false;
            } else if (!currentState.equals(InstanceStateName.Pending)) {
              return true;
            }
          }
        }
        AmazonServiceException exception = new AmazonServiceException(INVALID_INSTANCE_ID_NOT_FOUND);
        exception.setErrorCode(INVALID_INSTANCE_ID_NOT_FOUND);
        throw exception;
      };

      try {
        return retryUntil(task, timeout);
      } catch (RetryException e) {
        LOG.info("timeout waiting for instance {} to start", ec2InstanceId);
        throw new TimeoutException(String.format("Timeout waiting for instance %s to start", ec2InstanceId));
      } catch (ExecutionException e) {
        if (AmazonServiceException.class.isInstance(e.getCause())) {
          throw AWSExceptions.propagate((AmazonServiceException) e.getCause());
        }
        throw new UnrecoverableProviderException(e.getCause());
      }
    }

    @Override
    public Collection<EC2Instance> find(EC2InstanceTemplate template,
        Collection<String> instanceIds) throws InterruptedException {
      // Delegate to the provider so we get logging, etc.
      return EC2Provider.this.find(template, instanceIds);
    }

    @Override
    public void forEachInstance(DescribeInstancesResult result,
        Function<Instance, Void> instanceHandler) {
      List<Reservation> reservations;
      while (!(reservations = result.getReservations()).isEmpty()) {
        for (Reservation reservation : reservations) {
          for (Instance instance : reservation.getInstances()) {
            LOG.debug("Calling instance handler with instance {}", instance);
            instanceHandler.apply(instance);
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

    @Override
    public String getVirtualInstanceId(List<Tag> tags, String type) {
      String idTagName = ec2TagHelper.getClouderaDirectorIdTagName();
      for (Tag tag : tags) {
        if (tag.getKey().equals(idTagName)) {
          return tag.getValue();
        }
      }

      throw new IllegalStateException(String.format("Any %s managed by " +
          "Cloudera Altus Director should have a %s tag.", type, idTagName));
    }

    @Override
    public List<BlockDeviceMapping> getBlockDeviceMappings(EC2InstanceTemplate template) {
      // Query the AMI about the root device name & mapping information
      Image templateImage = getImage(template.getImage());
      String rootDeviceType = templateImage.getRootDeviceType();
      if (!DEVICE_TYPE_EBS.equals(rootDeviceType)) {
        throw new IllegalArgumentException("The root device for image " + template.getImage() +
            " must be \"" + DEVICE_TYPE_EBS + "\", found: " +
            rootDeviceType);
      }
      List<BlockDeviceMapping> originalMappings = templateImage.getBlockDeviceMappings();
      LOG.info(">> Original image block device mappings: {}", originalMappings);
      if (originalMappings.isEmpty()) {
        throw new IllegalArgumentException("The image " + template.getImage() +
            " has no block device mappings");
      }
      BlockDeviceMapping rootDevice = selectRootDevice(originalMappings,
          templateImage.getRootDeviceName());
      if (rootDevice == null) {
        throw new IllegalArgumentException("Could not determine root device for image " +
            template.getImage() + " based on root device name " +
            templateImage.getRootDeviceName());
      }

      Set<String> existingDeviceNames = getExistingDeviceNames(originalMappings);

      // The encrypted property was added to the block device mapping in version 1.8 of the SDK.
      // It is a Boolean, but defaults to false instead of being unset, so we set it to null here.
      rootDevice.getEbs().setEncrypted(null);
      rootDevice.getEbs().setVolumeSize(template.getRootVolumeSizeGB());
      rootDevice.getEbs().setVolumeType(template.getRootVolumeType());
      rootDevice.getEbs().setDeleteOnTermination(true);

      List<BlockDeviceMapping> deviceMappings = Lists.newArrayList(rootDevice);

      EBSAllocationStrategy ebsAllocationStrategy = EBSAllocationStrategy.get(template);

      switch (ebsAllocationStrategy) {
        case NO_EBS_VOLUMES:
          // The volumes within an instance should be homogeneous. So we only add
          // instance store volumes when additional EBS volumes aren't mounted.
          deviceMappings.addAll(ephemeralDeviceMappings.getBlockDeviceMappings(template.getType(), existingDeviceNames));
          break;
        case AS_INSTANCE_REQUEST:
          LOG.info("EBS volumes will be allocated as part of instance launch request");
          List<BlockDeviceMapping> mappings =
              ebsDeviceMappings.getBlockDeviceMappings(template, existingDeviceNames);
          deviceMappings.addAll(mappings);
          break;
        case AS_SEPARATE_REQUESTS:
          LOG.info("EBS volumes will be separately allocated after instance launch request");
          break;
        default:
          throw new IllegalStateException("Invalid EBS allocation strategy " + ebsAllocationStrategy);
      }

      LOG.info(">> Block device mappings: {}", deviceMappings);
      return deviceMappings;
    }

    @Override
    public InstanceNetworkInterfaceSpecification getInstanceNetworkInterfaceSpecification(
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

    @Override
    public boolean isAssociatePublicIpAddresses() {
      return associatePublicIpAddresses;
    }

    @Override
    public EC2Instance createInstance(EC2InstanceTemplate template, String instanceId, Instance instanceDetails) {
      return new EC2Instance(template, instanceId, instanceDetails);
    }

    @Override
    public Iterable<Entry<String, Instance>> doFind(
        EC2InstanceTemplate template, Iterable<String> instanceIds,
        Predicate<Instance> predicate)
        throws InterruptedException {
      return doFind(instanceIds, template, predicate, Duration.ZERO);
    }

    @SuppressWarnings({"StaticPseudoFunctionalStyleMethod"})
    @VisibleForTesting
    Iterable<Entry<String, Instance>> doFind(
        final Iterable<String> instanceIds,
        final EC2InstanceTemplate template,
        final Predicate<Instance> predicate,
        Duration timeout)
        throws InterruptedException {

      IdType idType = getIdType(template);

      final Map<String, Instance> result = Maps.newHashMap();
      for (String instanceId : instanceIds) {
        result.put(instanceId, null);
      }

      Retryer<Map<String, Instance>> retryer = RetryerBuilder.<Map<String, Instance>>newBuilder()
          .withStopStrategy(StopStrategies.stopAfterDelay(timeout.getMillis(), TimeUnit.MILLISECONDS))
          .withWaitStrategy(WaitStrategies.fixedWait(1000, TimeUnit.MILLISECONDS))
          .retryIfResult(idToInstances -> idToInstances != null &&
              !Iterables.all(idToInstances.entrySet(), ENTRY_VALUE_NOT_NULL))
          .build();

      try {
        retryer.call(() -> {
          Collection<String> ids = FluentIterable.from(result.entrySet())
              .filter(Predicates.not(ENTRY_VALUE_NOT_NULL))
              .transform(Entry::getKey)
              .toList();

          forEachInstance(ids, instance -> {
            if ((instance != null) && predicate.apply(instance)) {
              String instanceId = getInstanceId(template, instance, idType);
              result.put(instanceId, instance);
            }
            return null;
          }, idType);

          return result;
        });
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        Throwables.throwIfInstanceOf(cause, InterruptedException.class);
        Throwables.throwIfUnchecked(cause);
        throw new RuntimeException(cause);
      } catch (RetryException e) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }

      return Iterables.filter(result.entrySet(), ENTRY_VALUE_NOT_NULL);
    }

    /**
     * Iterates through the instances identified by the specified instance IDs and calls the
     * specified handler on each instance. Instances with the same instance IDs are deduplicated,
     * preferring running instances over terminating or terminated instances.
     *
     * @param instanceIds     the instance IDs
     * @param instanceHandler the instance handler
     * @param idType          the type of ID
     */
    @VisibleForTesting
    void forEachInstance(Collection<String> instanceIds,
        Function<Instance, Void> instanceHandler, IdType idType) {
      List<DescribeInstancesResult> results = Lists.newArrayList();
      String idTagName = ec2TagHelper.getClouderaDirectorIdTagName();
      for (List<String> instanceIdChunk : Iterables.partition(instanceIds, MAX_TAG_FILTERING_REQUESTS)) {
        DescribeInstancesResult result;
        switch (idType) {
          case EC2_INSTANCE_ID:
            try {
              result = client.describeInstances(new DescribeInstancesRequest()
                  .withInstanceIds(instanceIdChunk));
            } catch (AmazonServiceException e) {
              if (!"InvalidInstanceID.Malformed".equals(e.getErrorCode())) {
                throw e;
              }
              continue;
            }
            break;
          case VIRTUAL_INSTANCE_ID:
            result = client.describeInstances(new DescribeInstancesRequest()
                .withFilters(new Filter().withName("tag:" + idTagName)
                    .withValues(instanceIdChunk)));
            break;
          default:
            throw new IllegalStateException("Unknown ID type: " + idType);
        }
        results.add(result);
      }

      // collect Instances, preferring running instances over terminated instances and
      // also ensuring we only process one instance per instance id
      final Map<String, Instance> instanceIdToInstance =
          Maps.newHashMapWithExpectedSize(instanceIds.size());
      for (DescribeInstancesResult result : results) {
        forEachInstance(result, instance -> {
          String instanceId;
          try {
            instanceId = getInstanceId(null, instance, idType);
          } catch (IllegalStateException e) {
            LOG.error("Instance {} is not managed by Director. Skipping", instance.getInstanceId());
            return null;
          }
          Instance oldInstance = instanceIdToInstance.get(instanceId);
          if (oldInstance == null || INSTANCE_IS_TERMINAL.apply(oldInstance)) {
            instanceIdToInstance.put(instanceId, instance);
            if (oldInstance != null) {
              LOG.warn("Retaining new instance {} in preference to terminal instance {}",
                  instance, oldInstance);
            }
          } else {
            if (!INSTANCE_IS_TERMINAL.apply(instance)) {
              LOG.error("Two non-terminal instances with instance id {} exist: {} and {}",
                  instanceId, oldInstance, instance);
            } else {
              LOG.warn("Retaining current non-terminal instance {} in preference to terminal instance {}",
                  oldInstance, instance);
            }
          }
          return null;
        });
      }

      for (Instance instance : instanceIdToInstance.values()) {
        instanceHandler.apply(instance);
      }
    }

    @Override
    public void delete(EC2InstanceTemplate template, Collection<String> instanceIds)
        throws InterruptedException {

      Collection<String> ec2InstanceIds;

      if (isAutomatic(template)) {
        ec2InstanceIds = instanceIds;
      } else {
        ImmutableMap<Entry<String, Instance>, String> virtualInstanceIdToEC2InstanceIds = FluentIterable
            .from(doFind(template, instanceIds))
            .toMap(virtualInstanceIdToInstance -> {
              requireNonNull(virtualInstanceIdToInstance, "virtualInstanceIdToInstance is null");
              return virtualInstanceIdToInstance.getValue().getInstanceId();
            });

        if (virtualInstanceIdToEC2InstanceIds.size() < instanceIds.size()) {
          LOG.info("Unable to terminate instances, unknown {}",
              Sets.difference(Sets.newHashSet(instanceIds), virtualInstanceIdToEC2InstanceIds.keySet()));
        }

        ec2InstanceIds = virtualInstanceIdToEC2InstanceIds.values();
      }

      doDelete(ec2InstanceIds);
    }

    @Override
    public void doDelete(Collection<String> ec2InstanceIds) throws InterruptedException {
      if (ec2InstanceIds.isEmpty()) {
        return;
      }

      try {
        LOG.info(">> Terminating {}", ec2InstanceIds);
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(ec2InstanceIds);
        TerminateInstancesResult result = client.terminateInstances(request);
        LOG.info("<< Result {}", result);

      } catch (AmazonClientException e) {
        throw AWSExceptions.propagate(e);
      }
    }
  }
}

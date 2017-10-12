// (c) Copyright 2017 Cloudera, Inc.

package com.cloudera.director.aws.ec2;

import static com.cloudera.director.aws.ec2.EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.KEY_NAME;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PRIVATE_KEY;
import static com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_JCE_PUBLIC_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.AmazonEC2Client;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.cloudera.director.aws.shaded.com.amazonaws.services.ec2.model.KeyPairInfo;
import com.cloudera.director.aws.shaded.com.google.common.collect.ImmutableList;
import com.cloudera.director.spi.v2.model.Configured;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.util.KeySerialization;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * This test works on the parts of {@link EC2Provider} that pertain to key pair
 * handling.
 */
public class EC2ProviderKeyPairTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static String privateKeyString;
  private static String publicKeyString;
  private static String privateKeyFingerprint;
  private static String publicKeyFingerprint;

  @BeforeClass
  public static void setUpClass() throws Exception {
    KeyPairGenerator kpGenerator = KeyPairGenerator.getInstance("RSA");
    kpGenerator.initialize(1024);
    KeyPair kp = kpGenerator.genKeyPair();
    KeySerialization ks = new KeySerialization();
    privateKeyString = ks.serialize(kp.getPrivate());
    publicKeyString = ks.serialize(kp.getPublic());
    privateKeyFingerprint = EC2Provider.getSha1Fingerprint(kp.getPrivate());
    publicKeyFingerprint = EC2Provider.getMd5Fingerprint(kp.getPublic());
  }

  private EC2ProviderFixture fixture;
  private EC2Provider provider;

  @Before
  public void setUp() {
    fixture = new EC2ProviderFixture();
    provider = fixture.createEc2Provider();
  }

  @Test
  public void testEnhanceTemplateConfigurationWithoutKeys() {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(null);

    assertThat(provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class)))
        .isSameAs(configured);
    verify(configured, never()).getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class));
  }

  @Test
  public void testEnhanceTemplateConfigurationWithBothKeysPresentMatchingPublicKey() throws Exception {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(privateKeyString);
    when(configured.getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class)))
        .thenReturn(publicKeyString);
    when(configured.getConfiguration(any(LocalizationContext.class))).thenReturn(new HashMap<String, String>());

    AmazonEC2Client ec2Client = fixture.getAmazonEc2Client();
    DescribeKeyPairsResult result = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs()).thenReturn(result);
    KeyPairInfo info = mock(KeyPairInfo.class);
    when(result.getKeyPairs()).thenReturn(ImmutableList.of(info));
    when(info.getKeyName()).thenReturn("foundit");
    when(info.getKeyFingerprint()).thenReturn(publicKeyFingerprint);

    Configured enhancedConfiguration =
        provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class));
    assertThat(enhancedConfiguration.getConfiguration(fixture.getLocalizationContext()))
        .containsEntry(KEY_NAME.unwrap().getConfigKey(), "foundit");
  }

  @Test
  public void testEnhanceTemplateConfigurationWithOnlyPrivateKeyPresent() throws Exception {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(privateKeyString);
    when(configured.getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class)))
        .thenReturn(null);
    when(configured.getConfiguration(any(LocalizationContext.class))).thenReturn(new HashMap<String, String>());

    AmazonEC2Client ec2Client = fixture.getAmazonEc2Client();
    DescribeKeyPairsResult result = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs()).thenReturn(result);
    KeyPairInfo info = mock(KeyPairInfo.class);
    when(result.getKeyPairs()).thenReturn(ImmutableList.of(info));
    when(info.getKeyName()).thenReturn("foundit");
    when(info.getKeyFingerprint()).thenReturn(privateKeyFingerprint);

    Configured enhancedConfiguration =
        provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class));
    assertThat(enhancedConfiguration.getConfiguration(fixture.getLocalizationContext()))
        .containsEntry(KEY_NAME.unwrap().getConfigKey(), "foundit");
  }

  @Test
  public void testImportMissingKeyPair() throws Exception {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(privateKeyString);
    when(configured.getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class)))
        .thenReturn(publicKeyString);
    when(configured.getConfiguration(any(LocalizationContext.class))).thenReturn(new HashMap<String, String>());

    AmazonEC2Client ec2Client = fixture.getAmazonEc2Client();
    DescribeKeyPairsResult result = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs()).thenReturn(result);
    when(result.getKeyPairs()).thenReturn(ImmutableList.<KeyPairInfo>of());

    provider = fixture.createEc2Provider(true, "unittest-");
    Configured enhancedConfiguration =
        provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class));
    String expectedKeyName = "unittest-" + publicKeyFingerprint;
    assertThat(enhancedConfiguration.getConfiguration(fixture.getLocalizationContext()))
        .containsEntry(KEY_NAME.unwrap().getConfigKey(), expectedKeyName);

    ArgumentCaptor<ImportKeyPairRequest> requestCaptor = ArgumentCaptor.forClass(ImportKeyPairRequest.class);
    verify(ec2Client).importKeyPair(requestCaptor.capture());
    ImportKeyPairRequest request = requestCaptor.getValue();
    assertThat(request.getKeyName()).isEqualTo(expectedKeyName);
  }

  @Test
  public void testImportMissingKeyPairNotConfigured() throws Exception {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(privateKeyString);
    when(configured.getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class)))
        .thenReturn(publicKeyString);

    AmazonEC2Client ec2Client = fixture.getAmazonEc2Client();
    DescribeKeyPairsResult result = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs()).thenReturn(result);
    when(result.getKeyPairs()).thenReturn(ImmutableList.<KeyPairInfo>of());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No private key in EC2 matches the fingerprint");
    provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class));
  }

  @Test
  public void testImportMissingKeyPairWithoutIAMPermission() throws Exception {
    Configured configured = fixture.getConfigured();
    when(configured.getConfigurationValue(eq(SSH_JCE_PRIVATE_KEY), any(LocalizationContext.class)))
        .thenReturn(privateKeyString);
    when(configured.getConfigurationValue(eq(SSH_JCE_PUBLIC_KEY), any(LocalizationContext.class)))
        .thenReturn(publicKeyString);

    AmazonEC2Client ec2Client = fixture.getAmazonEc2Client();
    DescribeKeyPairsResult result = mock(DescribeKeyPairsResult.class);
    when(ec2Client.describeKeyPairs()).thenReturn(result);
    when(result.getKeyPairs()).thenReturn(ImmutableList.<KeyPairInfo>of());

    AmazonEC2Exception clientException = mock(AmazonEC2Exception.class);
    when(clientException.getErrorCode()).thenReturn("UnauthorizedOperation");
    when(ec2Client.importKeyPair(any(ImportKeyPairRequest.class))).thenThrow(clientException);

    thrown.expect(AmazonEC2Exception.class);
    thrown.expectMessage("add ec2:ImportKeyPair permission");
    provider = fixture.createEc2Provider(true, "unittest-");
    provider.enhanceTemplateConfiguration("testTemplate", configured, mock(LocalizationContext.class));
  }
}

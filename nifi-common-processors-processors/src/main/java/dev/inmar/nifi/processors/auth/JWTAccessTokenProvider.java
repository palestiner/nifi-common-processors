package dev.inmar.nifi.processors.auth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"jwt", "provider", "authorization", "access token", "http"})
public class JWTAccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider, VerifiableControllerService {

    public static final PropertyDescriptor SERVER_URL = new PropertyDescriptor.Builder()
            .name("server-url")
            .displayName("Server URL")
            .description("The URL of the server that issues access tokens.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("service-user-name")
            .displayName("Username")
            .description("Username on the service that is being accessed.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("service-password")
            .displayName("Password")
            .description("Password for the username on the service that is being accessed.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REFRESH_WINDOW = new PropertyDescriptor.Builder()
            .name("refresh-window")
            .displayName("Refresh Window")
            .description(
                    "The service will attempt to refresh tokens expiring within the refresh window, subtracting the configured duration from the token expiration.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 s")
            .required(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .addValidator(Validator.VALID)
            .identifiesControllerService(SSLContextService.class)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SERVER_URL,
            USERNAME,
            PASSWORD,
            REFRESH_WINDOW,
            SSL_CONTEXT
    );

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private volatile String serverUrl;
    private volatile OkHttpClient httpClient;
    private volatile String username;
    private volatile String password;
    private volatile long refreshWindowSeconds;

    private volatile AccessToken accessDetails;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        getProperties(context);
    }

    @OnDisabled
    public void onDisabled() {
        accessDetails = null;
    }

    protected OkHttpClient createHttpClient(ConfigurationContext context) {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        final SSLContextService sslContextProvider = context.getProperty(SSL_CONTEXT)
                .asControllerService(SSLContextService.class);
        if (sslContextProvider != null) {
            final X509TrustManager trustManager = sslContextProvider.createTrustManager();
            final SSLContext sslContext = sslContextProvider.createContext();
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        return clientBuilder.build();
    }

    @Override
    public AccessToken getAccessDetails() {
        if (this.accessDetails == null || isRefreshRequired()) {
            acquireAccessDetails();
        }
        return accessDetails;
    }

    private void getProperties(ConfigurationContext context) {
        serverUrl = context.getProperty(SERVER_URL)
                .evaluateAttributeExpressions()
                .getValue();

        httpClient = createHttpClient(context);

        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();

        refreshWindowSeconds = context.getProperty(REFRESH_WINDOW).asTimePeriod(TimeUnit.SECONDS);
    }

    private boolean isRefreshRequired() {
        final Instant expirationRefreshTime =
                Instant.ofEpochSecond(accessDetails.getExpiresIn())
                        .minusSeconds(refreshWindowSeconds);

        return Instant.now().isAfter(expirationRefreshTime);
    }

    private void acquireAccessDetails() {
        getLogger().debug("New Access Token request started [{}]", serverUrl);

        FormBody.Builder acquireTokenBuilder = new FormBody.Builder()
                .add("username", username)
                .add("password", password);

        this.accessDetails = requestToken(acquireTokenBuilder);
    }

    private AccessToken requestToken(FormBody.Builder formBuilder) {
        RequestBody requestBody = formBuilder.build();

        Request.Builder requestBuilder = new Request.Builder()
                .url(serverUrl)
                .post(requestBody);

        Request request = requestBuilder.build();

        return getAccessDetails(request);
    }

    private AccessToken getAccessDetails(final Request newRequest) {
        try {
            final Response response = httpClient.newCall(newRequest).execute();
            final String responseBody = response.body().string();
            if (response.isSuccessful()) {
                getLogger().debug("Nifi auth Access Token retrieved [HTTP {}]", response.code());
                if (responseBody.isEmpty()) {
                    throw new ProcessException("Nifi auth access token response is empty!");
                }
                String[] chunks = responseBody.split("\\.");
                Base64.Decoder decoder = Base64.getUrlDecoder();
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(decoder.decode(chunks[1]));
                final long expirationTimestamp = jsonNode.get("exp").asLong();
                final AccessToken accessToken = new AccessToken();
                accessToken.setAccessToken(responseBody);
                accessToken.setExpiresIn(expirationTimestamp); // Use as JWT exp (expiration) timestamp
                return accessToken;
            } else {
                getLogger().error(String.format(
                        "Nifi auth access token request failed [HTTP %d], response:%n%s",
                        response.code(),
                        responseBody
                ));
                throw new ProcessException(String.format(
                        "Nifi auth access token request failed [HTTP %d]",
                        response.code()
                ));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Nifi auth access token request failed", e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(
            ConfigurationContext context,
            ComponentLog verificationLogger,
            Map<String, String> variables
    ) {
        getProperties(context);

        ConfigVerificationResult.Builder builder = new ConfigVerificationResult.Builder()
                .verificationStepName("Can acquire token");

        try {
            getAccessDetails();
            builder.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL);
        } catch (Exception ex) {
            builder.outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(ex.getMessage());
        }

        return Arrays.asList(builder.build());
    }

}

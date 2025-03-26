package dev.inmar.nifi.processors.smb;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.smb.FetchSmb;
import org.apache.nifi.processors.smb.GetSmbFile;
import org.apache.nifi.processors.smb.ListSmb;
import org.apache.nifi.processors.smb.PutSmbFile;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static dev.inmar.nifi.processors.util.AttributeUtil.*;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.services.smb.SmbListableEntity.*;

@TriggerSerially
@Tags({"samba, smb, cifs, files", "list"})
@SeeAlso({PutSmbFile.class, GetSmbFile.class, FetchSmb.class})
@CapabilityDescription("Input allowed ListSmb processor version. " +
                       "This implementation is repeat code from ListSmb processor and changes two methods, " +
                       "onTrigger for check and set input flow file and " +
                       "createAttributes for copy input flow file attributes into any new generated flow files. " +
                       "For any details see original class implementation org.apache.nifi.processors.smb.ListSmb.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({
        @WritesAttribute(attribute = FILENAME, description = "The name of the file that was read from filesystem."),
        @WritesAttribute(attribute = SHORT_NAME, description = "The short name of the file that was read from filesystem."),
        @WritesAttribute(attribute = PATH, description =
                "The path is set to the relative path of the file's directory on the remote filesystem compared to the "
                + "Share root directory. For example, for a given remote location"
                + "smb://HOSTNAME:PORT/SHARE/DIRECTORY, and a file is being listed from "
                + "smb://HOSTNAME:PORT/SHARE/DIRECTORY/sub/folder/file then the path attribute will be set to "
                + "\"DIRECTORY/sub/folder\"."),
        @WritesAttribute(attribute = SERVICE_LOCATION, description =
                "The SMB URL of the share."),
        @WritesAttribute(attribute = LAST_MODIFIED_TIME, description =
                "The timestamp of when the file's content changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CREATION_TIME, description =
                "The timestamp of when the file was created in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = LAST_ACCESS_TIME, description =
                "The timestamp of when the file was accessed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CHANGE_TIME, description =
                "The timestamp of when the file's attributes was changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = SIZE, description = "The size of the file in bytes."),
        @WritesAttribute(attribute = ALLOCATION_SIZE, description = "The number of bytes allocated for the file on the server."),
})
@Stateful(scopes = {Scope.CLUSTER}, description =
        "After performing a listing of files, the state of the previous listing can be stored in order to list files "
        + "continuously without duplication."
)
public class InputListSmb extends AbstractListProcessor<SmbListableEntity> {

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            ListSmb.SMB_CLIENT_PROVIDER_SERVICE,
            ListSmb.SMB_LISTING_STRATEGY,
            ListSmb.DIRECTORY,
            ListSmb.FILE_NAME_SUFFIX_FILTER,
            AbstractListProcessor.RECORD_WRITER,
            ListSmb.MINIMUM_AGE,
            ListSmb.MAXIMUM_AGE,
            ListSmb.MINIMUM_SIZE,
            ListSmb.MAXIMUM_SIZE,
            AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET
    );

    private volatile Map<String, String> inputFlowFileAttributes;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        if (context.hasIncomingConnection()) {
            final FlowFile fileToProcess = session.get();
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            } else if (fileToProcess != null) {
                inputFlowFileAttributes = fileToProcess.getAttributes();
                session.remove(fileToProcess);
            }
        }

        super.onTrigger(context, session);
    }

    @Override
    protected Map<String, String> createAttributes(SmbListableEntity entity, ProcessContext context) {
        final Map<String, String> attributes = new TreeMap<>();
        final SmbClientProviderService clientProviderService =
                context.getProperty(ListSmb.SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        attributes.put(FILENAME, entity.getName());
        attributes.put(SHORT_NAME, entity.getShortName());
        attributes.put(PATH, entity.getPath());
        attributes.put(SERVICE_LOCATION, clientProviderService.getServiceLocation().toString());
        attributes.put(LAST_MODIFIED_TIME, formatTimeStamp(entity.getLastModifiedTime()));
        attributes.put(CREATION_TIME, formatTimeStamp(entity.getCreationTime()));
        attributes.put(LAST_ACCESS_TIME, formatTimeStamp(entity.getLastAccessTime()));
        attributes.put(CHANGE_TIME, formatTimeStamp(entity.getChangeTime()));
        attributes.put(SIZE, String.valueOf(entity.getSize()));
        attributes.put(ALLOCATION_SIZE, String.valueOf(entity.getAllocationSize()));
        putAllUnique(attributes, inputFlowFileAttributes);
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    protected String getPath(ProcessContext context) {
        final SmbClientProviderService clientProviderService =
                context.getProperty(ListSmb.SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final URI serviceLocation = clientProviderService.getServiceLocation();
        final String directory = getDirectory(context);
        return String.format("%s/%s", serviceLocation.toString(), directory.isEmpty() ? "" : directory + "/");
    }

    @Override
    protected List<SmbListableEntity> performListing(ProcessContext context, Long minimumTimestampOrNull,
                                                     AbstractListProcessor.ListingMode listingMode) throws IOException {

        final Predicate<SmbListableEntity> fileFilter =
                createFileFilter(context, minimumTimestampOrNull);

        try (Stream<SmbListableEntity> listing = performListing(context)) {
            final Iterator<SmbListableEntity> iterator = listing.iterator();
            final List<SmbListableEntity> result = new LinkedList<>();
            while (iterator.hasNext()) {
                if (isExecutionStopped(listingMode)) {
                    return emptyList();
                }
                final SmbListableEntity entity = iterator.next();
                if (fileFilter.test(entity)) {
                    result.add(entity);
                }
            }
            return result;
        } catch (Exception e) {
            throw new IOException("Could not perform listing", e);
        }
    }

    @Override
    protected boolean isListingResetNecessary(PropertyDescriptor property) {
        return asList(ListSmb.SMB_CLIENT_PROVIDER_SERVICE, ListSmb.DIRECTORY, ListSmb.FILE_NAME_SUFFIX_FILTER).contains(property);
    }

    @Override
    protected Scope getStateScope(PropertyContext context) {
        return CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return SmbListableEntity.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(ProcessContext context) throws IOException {
        try (Stream<SmbListableEntity> listing = performListing(context)) {
            return Long.valueOf(listing.count()).intValue();
        } catch (Exception e) {
            throw new IOException("Could not count files", e);
        }
    }

    @Override
    protected String getListingContainerName(ProcessContext context) {
        return String.format("Remote Directory [%s]", getPath(context));
    }

    private String formatTimeStamp(long timestamp) {
        return ISO_DATE_TIME.format(
                LocalDateTime.ofEpochSecond(MILLISECONDS.toSeconds(timestamp), 0, UTC));
    }

    private boolean isExecutionStopped(AbstractListProcessor.ListingMode listingMode) {
        return AbstractListProcessor.ListingMode.EXECUTION.equals(listingMode) && !isScheduled();
    }

    private Predicate<SmbListableEntity> createFileFilter(ProcessContext context, Long minTimestampOrNull) {

        final Long minimumAge = context.getProperty(ListSmb.MINIMUM_AGE).asTimePeriod(MILLISECONDS);
        final Long maximumAgeOrNull = context.getProperty(ListSmb.MAXIMUM_AGE).isSet() ? context.getProperty(ListSmb.MAXIMUM_AGE)
                .asTimePeriod(MILLISECONDS) : null;
        final Double minimumSizeOrNull =
                context.getProperty(ListSmb.MINIMUM_SIZE).isSet() ? context.getProperty(ListSmb.MINIMUM_SIZE).asDataSize(DataUnit.B)
                                                          : null;
        final Double maximumSizeOrNull =
                context.getProperty(ListSmb.MAXIMUM_SIZE).isSet() ? context.getProperty(ListSmb.MAXIMUM_SIZE).asDataSize(DataUnit.B)
                                                          : null;
        final String suffixOrNull = context.getProperty(ListSmb.FILE_NAME_SUFFIX_FILTER).getValue();

        final long now = getCurrentTime();
        Predicate<SmbListableEntity> filter = entity -> now - entity.getLastModifiedTime() >= minimumAge;

        if (maximumAgeOrNull != null) {
            filter = filter.and(entity -> now - entity.getLastModifiedTime() <= maximumAgeOrNull);
        }

        if (minTimestampOrNull != null) {
            filter = filter.and(entity -> entity.getLastModifiedTime() >= minTimestampOrNull);
        }

        if (minimumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() >= minimumSizeOrNull);
        }

        if (maximumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() <= maximumSizeOrNull);
        }

        if (suffixOrNull != null) {
            filter = filter.and(entity -> !entity.getName().endsWith(suffixOrNull));
        }

        return filter;
    }

    private Stream<SmbListableEntity> performListing(ProcessContext context) throws IOException {
        final SmbClientProviderService clientProviderService =
                context.getProperty(ListSmb.SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final String directory = getDirectory(context);
        final SmbClientService clientService = clientProviderService.getClient();
        return clientService.listFiles(directory).onClose(() -> {
            try {
                clientService.close();
            } catch (Exception e) {
                throw new RuntimeException("Could not close SMB client", e);
            }
        });
    }

    private String getDirectory(ProcessContext context) {
        final PropertyValue property = context.getProperty(ListSmb.DIRECTORY);
        final String directory = property.isSet() ? property.getValue().replace('\\', '/') : "";
        return "/".equals(directory) ? "" : directory;
    }

}

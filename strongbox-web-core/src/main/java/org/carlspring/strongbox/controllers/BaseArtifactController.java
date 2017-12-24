package org.carlspring.strongbox.controllers;

import org.carlspring.maven.commons.util.ArtifactUtils;
import org.carlspring.strongbox.client.ArtifactTransportException;
import org.carlspring.strongbox.event.artifact.ArtifactEventListenerRegistry;
import org.carlspring.strongbox.io.ArtifactInputStream;
import org.carlspring.strongbox.providers.datastore.StorageProviderRegistry;
import org.carlspring.strongbox.providers.layout.LayoutProvider;
import org.carlspring.strongbox.providers.layout.LayoutProviderRegistry;
import org.carlspring.strongbox.services.ArtifactManagementService;
import org.carlspring.strongbox.storage.ArtifactResolutionException;
import org.carlspring.strongbox.storage.Storage;
import org.carlspring.strongbox.storage.repository.Repository;
import org.carlspring.strongbox.utils.ArtifactControllerHelper;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;

import io.swagger.annotations.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.DirectoryFileComparator;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import static org.carlspring.strongbox.utils.ArtifactControllerHelper.handlePartialDownload;
import static org.carlspring.strongbox.utils.ArtifactControllerHelper.isRangedRequest;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Api(value = "/storages")
public abstract class BaseArtifactController
        extends BaseController
{

    @Inject
    private LayoutProviderRegistry layoutProviderRegistry;

    @Inject
    private StorageProviderRegistry storageProviderRegistry;

    @Inject
    protected ArtifactEventListenerRegistry artifactEventListenerRegistry;


    @ApiOperation(value = "Used to deploy an artifact", position = 0)
    @ApiResponses(value = { @ApiResponse(code = 200, message = "The artifact was deployed successfully."),
                            @ApiResponse(code = 400, message = "An error occurred.") })
    @PreAuthorize("hasAuthority('ARTIFACTS_DEPLOY')")
    @RequestMapping(value = "{storageId}/{repositoryId}/{path:.+}", method = RequestMethod.PUT)
    public ResponseEntity upload(@ApiParam(value = "The storageId", required = true)
                                 @PathVariable(name = "storageId") String storageId,
                                 @ApiParam(value = "The repositoryId", required = true)
                                 @PathVariable(name = "repositoryId") String repositoryId,
                                 @PathVariable String path,
                                 HttpServletRequest request)
    {
        try
        {
            getArtifactManagementService(storageId, repositoryId).validateAndStore(storageId,
                                                                                   repositoryId,
                                                                                   path,
                                                                                   request.getInputStream());

            return ResponseEntity.ok("The artifact was deployed successfully.");
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @ApiOperation(value = "Used to retrieve an artifact", position = 1)
    @ApiResponses(value = { @ApiResponse(code = 200, message = ""),
                            @ApiResponse(code = 400, message = "An error occurred.") })
    @PreAuthorize("hasAuthority('ARTIFACTS_RESOLVE')")
    @RequestMapping(value = { "{storageId}/{repositoryId}/{path:.+}" }, method = RequestMethod.GET)
    public void download(@ApiParam(value = "The storageId", required = true)
                         @PathVariable String storageId,
                         @ApiParam(value = "The repositoryId", required = true)
                         @PathVariable String repositoryId,
                         @RequestHeader HttpHeaders httpHeaders,
                         @PathVariable String path,
                         HttpServletRequest request,
                         HttpServletResponse response)
            throws Exception
    {
        logger.debug("Requested /" + storageId + "/" + repositoryId + "/" + path + ".");

        Storage storage = configurationManager.getConfiguration().getStorage(storageId);
        if (storage == null)
        {
            logger.error("Unable to find storage by ID " + storageId);

            response.sendError(INTERNAL_SERVER_ERROR.value(), "Unable to find storage by ID " + storageId);

            return;
        }

        Repository repository = storage.getRepository(repositoryId);
        if (repository == null)
        {
            logger.error("Unable to find repository by ID " + repositoryId + " for storage " + storageId);

            response.sendError(INTERNAL_SERVER_ERROR.value(),
                               "Unable to find repository by ID " + repositoryId + " for storage " + storageId);
            return;
        }

        if (!repository.isInService())
        {
            logger.error("Repository is not in service...");

            response.setStatus(HttpStatus.SERVICE_UNAVAILABLE.value());

            return;
        }

        if (repository.allowsDirectoryBrowsing() && probeForDirectoryListing(repository, path))
        {
            try
            {
                generateDirectoryListing(repository, path, request, response);
            }
            catch (Exception e)
            {
                logger.debug("Unable to generate directory listing for " +
                             "/" + storageId + "/" + repositoryId + "/" + path, e);

                response.setStatus(INTERNAL_SERVER_ERROR.value());
            }

            return;
        }

        ArtifactInputStream is;
        try
        {
            is = (ArtifactInputStream) getArtifactManagementService(storageId, repositoryId).resolve(storageId,
                                                                                                     repositoryId,
                                                                                                     path);
            if (is == null)
            {
                response.setStatus(NOT_FOUND.value());
                return;
            }

            if (isRangedRequest(httpHeaders))
            {
                logger.debug("Detecting range request....");

                handlePartialDownload(is, httpHeaders, response);
            }

            artifactEventListenerRegistry.dispatchArtifactDownloadingEvent(storage.getId(), repository.getId(), path);

            copyToResponse(is, response);

            artifactEventListenerRegistry.dispatchArtifactDownloadedEvent(storage.getId(), repository.getId(), path);
        }
        catch (ArtifactResolutionException | ArtifactTransportException e)
        {
            logger.debug("Unable to find artifact by path " + path, e);

            response.setStatus(NOT_FOUND.value());

            return;
        }

        setMediaTypeHeader(path, response);

        response.setHeader("Accept-Ranges", "bytes");

        ArtifactControllerHelper.setHeadersForChecksums(is, response);

        logger.debug("Download succeeded.");
    }

    private void setMediaTypeHeader(String path,
                                    HttpServletResponse response)
    {
        // TODO: This is far from optimal and will need to have a content type approach at some point:
        if (ArtifactUtils.isChecksum(path))
        {
            response.setContentType(MediaType.TEXT_PLAIN_VALUE);
        }
        else if (ArtifactUtils.isMetadata(path))
        {
            response.setContentType(MediaType.APPLICATION_XML_VALUE);
        }
        else
        {
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        }
    }

    private boolean probeForDirectoryListing(Repository repository,
                                             String path)
    {
        String filePath = path.replaceAll("/", Matcher.quoteReplacement(File.separator));

        String dir = repository.getBasedir() + File.separator + filePath;

        File file = new File(dir);

        // Do not allow .index and .trash directories (or any other directory starting with ".") to be browseable.
        // NB: Files will still be downloadable.
        if (!file.isHidden() && !path.startsWith(".") && !path.contains("/."))
        {
            if (file.exists() && file.isDirectory())
            {
                return true;
            }

            file = new File(dir + File.separator);

            return file.exists() && file.isDirectory();
        }
        else
        {
            return false;
        }
    }

    private void generateDirectoryListing(Repository repository,
                                          String path,
                                          HttpServletRequest request,
                                          HttpServletResponse response)
    {
        path = path.replaceAll("/", Matcher.quoteReplacement(File.separator));

        if (request == null)
        {
            throw new RuntimeException("Unable to retrieve HTTP request from execution context");
        }

        String dir = repository.getBasedir() + File.separator + path;
        String requestUri = request.getRequestURI();

        File file = new File(dir);

        if (file.isDirectory() && !requestUri.endsWith("/"))
        {
            response.setLocale(new Locale(request.getRequestURI() + "/"));
            response.setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        }

        try
        {
            logger.debug(" browsing: " + file.toString());

            StringBuilder sb = new StringBuilder();
            sb.append("<html>");
            sb.append("<head>");
            sb.append(
                    "<style>body{font-family: \"Trebuchet MS\", verdana, lucida, arial, helvetica, sans-serif;} table tr {text-align: left;}</style>");
            sb.append("<title>Index of " + request.getRequestURI() + "</title>");
            sb.append("</head>");
            sb.append("<body>");
            sb.append("<h1>Index of " + request.getRequestURI() + "</h1>");
            sb.append("<table cellspacing=\"10\">");
            sb.append("<tr>");
            sb.append("<th>Name</th>");
            sb.append("<th>Last modified</th>");
            sb.append("<th>Size</th>");
            sb.append("</tr>");
            sb.append("<tr>");
            sb.append("<td colspan=3><a href='..'>..</a></td>");
            sb.append("</tr>");

            File[] childFiles = file.listFiles();
            if (childFiles != null)
            {
                Arrays.sort(childFiles, DirectoryFileComparator.DIRECTORY_COMPARATOR);

                for (File childFile : childFiles)
                {
                    String name = childFile.getName();

                    if (name.startsWith(".") || childFile.isHidden())
                    {
                        continue;
                    }

                    String lastModified = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss").format(
                            new Date(childFile.lastModified()));

                    sb.append("<tr>");
                    sb.append("<td><a href='" + URLEncoder.encode(name, "UTF-8") + (childFile.isDirectory() ?
                                                                                    "/" : "") + "'>" + name +
                              (childFile.isDirectory() ? "/" : "") + "</a></td>");
                    sb.append("<td>" + lastModified + "</td>");
                    sb.append("<td>" + FileUtils.byteCountToDisplaySize(childFile.length()) + "</td>");
                    sb.append("</tr>");
                }
            }

            sb.append("</table>");
            sb.append("</body>");
            sb.append("</html>");

            response.setContentType("text/html;charset=UTF-8");
            response.setStatus(HttpStatus.FOUND.value());
            response.getWriter()
                    .write(sb.toString());
            response.getWriter()
                    .flush();
            response.getWriter()
                    .close();

        }
        catch (Exception e)
        {
            logger.error(" error accessing requested directory: " + file.getAbsolutePath(), e);

            response.setStatus(404);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Common-purpose methods

    public Storage getStorage(String storageId)
    {
        return configurationManager.getConfiguration().getStorage(storageId);
    }

    public Repository getRepository(String storageId,
                                    String repositoryId)
    {
        return getStorage(storageId).getRepository(repositoryId);
    }

    public LayoutProviderRegistry getLayoutProviderRegistry()
    {
        return layoutProviderRegistry;
    }

    public void setLayoutProviderRegistry(LayoutProviderRegistry layoutProviderRegistry)
    {
        this.layoutProviderRegistry = layoutProviderRegistry;
    }

    public StorageProviderRegistry getStorageProviderRegistry()
    {
        return storageProviderRegistry;
    }

    public void setStorageProviderRegistry(StorageProviderRegistry storageProviderRegistry)
    {
        this.storageProviderRegistry = storageProviderRegistry;
    }

    public ArtifactManagementService getArtifactManagementService(String storageId,
                                                                  String repositoryId)
    {
        Storage storage = getConfiguration().getStorage(storageId);
        Repository repository = storage.getRepository(repositoryId);

        LayoutProvider layoutProvider = layoutProviderRegistry.getProvider(repository.getLayout());

        return layoutProvider.getArtifactManagementService();
    }

}

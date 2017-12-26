package org.carlspring.strongbox.controllers;

import org.carlspring.maven.commons.util.ArtifactUtils;
import org.carlspring.strongbox.event.artifact.ArtifactEventListenerRegistry;
import org.carlspring.strongbox.providers.datastore.StorageProviderRegistry;
import org.carlspring.strongbox.providers.layout.LayoutProvider;
import org.carlspring.strongbox.providers.layout.LayoutProviderRegistry;
import org.carlspring.strongbox.services.ArtifactManagementService;
import org.carlspring.strongbox.storage.Storage;
import org.carlspring.strongbox.storage.repository.Repository;

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

import io.swagger.annotations.Api;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.DirectoryFileComparator;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

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


    protected void setMediaTypeHeader(String path,
                                      HttpServletResponse response)
    {
        // TODO: This is far from optimal and will need to have a proper content type approach at some point:
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

    protected boolean probeForDirectoryListing(Repository repository,
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

    protected void generateDirectoryListing(Repository repository,
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

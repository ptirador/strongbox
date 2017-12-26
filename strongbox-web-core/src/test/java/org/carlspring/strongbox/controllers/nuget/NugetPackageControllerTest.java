package org.carlspring.strongbox.controllers.nuget;

import static io.restassured.module.mockmvc.RestAssuredMockMvc.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.carlspring.strongbox.configuration.ConfigurationManager;
import org.carlspring.strongbox.controllers.context.IntegrationTest;
import org.carlspring.strongbox.domain.ArtifactEntry;
import org.carlspring.strongbox.domain.RemoteArtifactEntry;
import org.carlspring.strongbox.rest.common.NugetRestAssuredBaseTest;
import org.carlspring.strongbox.services.ArtifactEntryService;
import org.carlspring.strongbox.storage.repository.Repository;
import org.carlspring.strongbox.storage.repository.RepositoryPolicyEnum;
import org.carlspring.strongbox.xml.configuration.repository.MavenRepositoryConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.restassured.module.mockmvc.config.RestAssuredMockMvcConfig;
import io.restassured.module.mockmvc.specification.MockMvcRequestSpecification;

/**
 * @author Sergey Bespalov
 *
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
public class NugetPackageControllerTest extends NugetRestAssuredBaseTest
{

    private static final String API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJTdHJvbmdib3giLCJqdGkiOiJ0SExSbWU4eFJOSnJjNXVXdTVkZDhRIiwic3ViIjoiYWRtaW4iLCJzZWN1cml0eS10b2tlbi1rZXkiOiJhZG1pbi1zZWNyZXQifQ.xRWxXt5yob5qcHjsvV1YsyfY3C-XFt9oKPABY0tYx88";

    private final static String STORAGE_ID = "storage-nuget-test";

    private static final String REPOSITORY_RELEASES_1 = "nuget-releases-1";

    @Inject
    private ConfigurationManager configurationManager;

    @Inject
    private ArtifactEntryService artifactEntryService;

    @BeforeClass
    public static void cleanUp()
        throws Exception
    {
        cleanUp(getRepositoriesToClean());
    }

    public static Set<Repository> getRepositoriesToClean()
    {
        Set<Repository> repositories = new LinkedHashSet<>();
        repositories.add(createRepositoryMock(STORAGE_ID, REPOSITORY_RELEASES_1));

        return repositories;
    }

    @Override
    public void init()
        throws Exception
    {
        super.init();

        RestAssuredMockMvcConfig config = RestAssuredMockMvcConfig.config();
        config.getLogConfig().enableLoggingOfRequestAndResponseIfValidationFails();
        given().config(config);

        createStorage(STORAGE_ID);

        MavenRepositoryConfiguration mavenRepositoryConfiguration = new MavenRepositoryConfiguration();
        mavenRepositoryConfiguration.setIndexingEnabled(false);

        Repository repository1 = new Repository(REPOSITORY_RELEASES_1);
        repository1.setPolicy(RepositoryPolicyEnum.RELEASE.getPolicy());
        repository1.setStorage(configurationManager.getConfiguration().getStorage(STORAGE_ID));
        repository1.setLayout("Nuget Hierarchical");
        repository1.setRepositoryConfiguration(mavenRepositoryConfiguration);

        createRepository(repository1);
    }

    @Test
    public void testPackageDelete()
        throws Exception
    {
        String packageId = "Org.Carlspring.Strongbox.Examples.Nuget.Mono.Delete";
        String packageVersion = "1.0.0";
        Path packageFile = generatePackageFile(packageId, packageVersion);
        byte[] packageContent = readPackageContent(packageFile);

        // Push
        createPushRequest(packageContent).when()
                                         .put(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" +
                                              REPOSITORY_RELEASES_1 + "/")
                                         .peek()
                                         .then()
                                         .statusCode(HttpStatus.CREATED.value());

        // Delete
        given().header("User-Agent", "NuGet/*")
               .header("X-NuGet-ApiKey", API_KEY)
               .when()
               .delete(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 + "/" +
                       packageId + "/" + packageVersion)
               .peek()
               .then()
               .statusCode(HttpStatus.OK.value());
    }

    @Test
    public void testPackageCommonFlow()
        throws Exception
    {
        String packageId = "Org.Carlspring.Strongbox.Examples.Nuget.Mono";
        String packageVersion = "1.0.0";
        Path packageFile = generatePackageFile(packageId, packageVersion);
        long packageSize = Files.size(packageFile);
        byte[] packageContent = readPackageContent(packageFile);

        // Push
        createPushRequest(packageContent)
               .when()
               .put(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 + "/")
               .peek()
               .then()
               .statusCode(HttpStatus.CREATED.value());

        //Find by ID
        given().header("User-Agent", "NuGet/*")
               .when()
               .get(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 +
                       "/FindPackagesById()?id='Org.Carlspring.Strongbox.Examples.Nuget.Mono'")
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .assertThat()
               .body("feed.title", equalTo("Packages"))
               .and()
               .assertThat()
               .body("feed.entry[0].title", equalTo("Org.Carlspring.Strongbox.Examples.Nuget.Mono"));

        // We need to mute `System.out` here manually because response body logging hardcoded in current version of
        // RestAssured, and we can not change it using configuration (@see `RestAssuredResponseOptionsGroovyImpl.peek(...)`).
        PrintStream originalSysOut = muteSystemOutput();
        try
        {
            // Get1
            given().header("User-Agent", "NuGet/*")
                   .when()
                   .get(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 + "/download/" +
                        packageId + "/" + packageVersion)
                   .peek()
                   .then()
                   .statusCode(HttpStatus.OK.value())
                   .assertThat()
                   .header("Content-Length", equalTo(String.valueOf(packageSize)));
            
            // Get2
            given().header("User-Agent", "NuGet/*")
                   .when()
                   .get(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 + "/" +
                        packageId + "/" + packageVersion)
                   .peek()
                   .then()
                   .statusCode(HttpStatus.OK.value())
                   .assertThat()
                   .header("Content-Length", equalTo(String.valueOf(packageSize)));
        }
        finally
        {
            System.setOut(originalSysOut);
        }
    }

    /**
     * Mute the system output to avoid malicious logging (binary content for example).
     *
     * @return
     */
    private PrintStream muteSystemOutput()
    {
        PrintStream original = System.out;
        System.setOut(new PrintStream(new OutputStream()
        {
            public void write(int b)
            {
                //DO NOTHING
            }
        }));

        return original;
    }

    @Test
    public void testPackageSearch()
        throws Exception
    {
        String packageId = "Org.Carlspring.Strongbox.Nuget.Test.Search";
        String packageVersion = "1.0.0";
        byte[] packageContent = readPackageContent(generatePackageFile(packageId, packageVersion));

        // Push
        createPushRequest(packageContent).when()
                                         .put(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" +
                                              REPOSITORY_RELEASES_1 + "/")
                                         .peek()
                                         .then()
                                         .statusCode(HttpStatus.CREATED.value());

        // Count
        given().header("User-Agent", "NuGet/*")
               .when()
               .get(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 +
                    String.format("/Search()/$count?$filter=%s&searchTerm=%s&targetFramework=",
                                  "IsLatestVersion", "Test"))
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .assertThat()
               .body(equalTo("1"));

        // Search
        given().header("User-Agent", "NuGet/*")
               .when()
               .get(getContextBaseUrl() + "/storages/" + STORAGE_ID + "/" + REPOSITORY_RELEASES_1 +
                    String.format("/Search()?$filter=%s&$skip=%s&$top=%s&searchTerm=%s&targetFramework=",
                                  "IsLatestVersion", 0, 30, "Test"))
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .assertThat()
               .body("feed.title", equalTo("Packages"))
               .and()
               .assertThat()
               .body("feed.entry[0].title", equalTo("Org.Carlspring.Strongbox.Nuget.Test.Search"));
    }

    public MockMvcRequestSpecification createPushRequest(byte[] packageContent)
    {
        return given().header("User-Agent", "NuGet/*")
                      .header("X-NuGet-ApiKey", API_KEY)
                      .header("Content-Type", "multipart/form-data; boundary=---------------------------123qwe")
                      .body(packageContent);
    }
    
    @Test
    public void testRemoteProxyGroup()
            throws Exception
    {
        given().header("User-Agent", "NuGet/*")
               .when()
               .get(getContextBaseUrl() + "/storages/public/nuget-public/FindPackagesById()?id=NHibernate")
               .then()
               .statusCode(HttpStatus.OK.value())
               .and()
               .assertThat()
               .body("feed.title", equalTo("Packages"))
               .and()
               .assertThat()
               .body("feed.entry[0].title", equalTo("NHibernate"));
        
        Map<String, String> coordinates = new HashMap<>();
        coordinates.put("id", "NHibernate");
        coordinates.put("version", "4.1.1.4000");

        List<ArtifactEntry> artifactEntryList = artifactEntryService.findArtifactList("storage-common-proxies",
                                                                                      "nuget.org",
                                                                                      coordinates);
        assertTrue(artifactEntryList.size() > 0);
        
        ArtifactEntry artifactEntry = artifactEntryList.iterator().next();
        assertTrue(artifactEntry instanceof RemoteArtifactEntry);
        assertFalse(((RemoteArtifactEntry)artifactEntry).getIsCached());
        
        PrintStream originalSysOut = muteSystemOutput();
        try
        {
            given().header("User-Agent", "NuGet/*")
                   .when()
                   .get(getContextBaseUrl() + "/storages/public/nuget-public/package/NHibernate/4.1.1.4000")
                   .peek()
                   .then()
                   .statusCode(HttpStatus.OK.value())
                   .assertThat()
                   .header("Content-Length", equalTo(String.valueOf(1490223)));
        }
        finally
        {
            System.setOut(originalSysOut);
        }
    }

}

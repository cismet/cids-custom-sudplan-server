/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.encoder.GSLayerEncoder;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import de.cismet.tools.PasswordEncrypter;
import de.cismet.tools.PropertyReader;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class SwmmResultGeoserverUpdater {

    //~ Static fields/initializers ---------------------------------------------

    public static final Logger LOG = Logger.getLogger(SwmmResultGeoserverUpdater.class);

    private static final PropertyReader propertyReader;
    private static final String FILE_PROPERTY = "/de/cismet/cids/custom/sudplan/server/trigger/geoserver.properties";

    public static final String CREATE_VIEW_STATEMENT_TEMPLATE;
    public static final String GEOSERVER_DATASTORE;
    public static final String GEOSERVER_WORKSPACE;
    public static final String GEOSERVER_SLD;
    public static final String VIEW_NAME_BASE;
    public static final String BB_QUERY;
    public static final String CRS;
    public static final String SRS;

    static {
        propertyReader = new PropertyReader(FILE_PROPERTY);
        CREATE_VIEW_STATEMENT_TEMPLATE = propertyReader.getProperty("CREATE_VIEW_STATEMENT_TEMPLATE");
        GEOSERVER_DATASTORE = propertyReader.getProperty("GEOSERVER_DATASTORE");
        GEOSERVER_WORKSPACE = propertyReader.getProperty("GEOSERVER_WORKSPACE");
        GEOSERVER_SLD = propertyReader.getProperty("GEOSERVER_SLD");
        VIEW_NAME_BASE = propertyReader.getProperty("VIEW_NAME_BASE");
        BB_QUERY = propertyReader.getProperty("BB_QUERY") + VIEW_NAME_BASE;
        CRS = propertyReader.getProperty("CRS");
        SRS = propertyReader.getProperty("SRS");
    }

    //~ Instance fields --------------------------------------------------------

    private final String restUser;
    private final String restPassword;
    private final String restUrl;
    private final Connection dbConnection;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SwmmResultToGeoserverLayer object.
     *
     * @param  dbConnection  DOCUMENT ME!
     */
    public SwmmResultGeoserverUpdater(final Connection dbConnection) {
        restUser = propertyReader.getProperty("restUser");
        restPassword = String.valueOf(PasswordEncrypter.decrypt(
                    propertyReader.getProperty("restPassword").toCharArray(),
                    true));
        restUrl = propertyReader.getProperty("restUrl");
        this.dbConnection = dbConnection;
    }

    /**
     * Creates a new SwmmResultToGeoserverLayer object.
     *
     * @param  restUser      DOCUMENT ME!
     * @param  restPassword  DOCUMENT ME!
     * @param  restUrl       DOCUMENT ME!
     * @param  dbConnection  dbUser DOCUMENT ME!
     */
    public SwmmResultGeoserverUpdater(
            final String restUser,
            final String restPassword,
            final String restUrl,
            final Connection dbConnection) {
        this.restUser = restUser;
        this.restPassword = restPassword;
        this.restUrl = restUrl;
        this.dbConnection = dbConnection;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   swmmRunId    DOCUMENT ME!
     * @param   swmmRunName  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public void importToGeoServer(final int swmmRunId,
            final String swmmRunName) throws Exception {
        final String viewName = VIEW_NAME_BASE + swmmRunId;
        LOG.info("creating view '" + viewName + "' for SWMM RUN '" + swmmRunName
                    + "' on geoserver instance '" + this.restUrl + "'");

        final String createViewSQL = (CREATE_VIEW_STATEMENT_TEMPLATE.replaceAll("%VIEW%", String.valueOf(viewName)))
                    + swmmRunId + ';';
        if (LOG.isDebugEnabled()) {
            LOG.debug(createViewSQL);
        }

        final Statement statement = dbConnection.createStatement();
        statement.execute(createViewSQL);

        final GeoServerRESTPublisher publisher = new GeoServerRESTPublisher(
                this.restUrl,
                this.restUser,
                this.restPassword);

        final AttributesAwareGSFeatureTypeEncoder featureType = new AttributesAwareGSFeatureTypeEncoder();
        featureType.setName(viewName); // view name
        featureType.setTitle(swmmRunName);
        featureType.setEnabled(true);
        featureType.setSRS(SRS);
        featureType.setProjectionPolicy(GSResourceEncoder.ProjectionPolicy.FORCE_DECLARED);

        GSAttributeEncoder attribute = new GSAttributeEncoder();
        attribute.addEntry("name", "geom");
        attribute.addEntry("minOccurs", "0");
        attribute.addEntry("maxOccurs", "1");
        attribute.addEntry("nillable", "false");
        attribute.addEntry("binding", "com.vividsolutions.jts.geom.Geometry");
        featureType.addAttribute(attribute);

        attribute = new GSAttributeEncoder();
        attribute.addEntry("name", "name");
        attribute.addEntry("minOccurs", "0");
        attribute.addEntry("maxOccurs", "1");
        attribute.addEntry("nillable", "true");
        attribute.addEntry("binding", "java.lang.String");
        featureType.addAttribute(attribute);

        attribute = new GSAttributeEncoder();
        attribute.addEntry("name", "scenario_name");
        attribute.addEntry("minOccurs", "0");
        attribute.addEntry("maxOccurs", "1");
        attribute.addEntry("nillable", "true");
        attribute.addEntry("binding", "java.lang.String");
        featureType.addAttribute(attribute);

        attribute = new GSAttributeEncoder();
        attribute.addEntry("name", "overflow_volume");
        attribute.addEntry("minOccurs", "0");
        attribute.addEntry("maxOccurs", "1");
        attribute.addEntry("nillable", "true");
        attribute.addEntry("binding", "java.lang.Float");
        featureType.addAttribute(attribute);

        final String getBBoxSQL = BB_QUERY + swmmRunId;
        if (LOG.isDebugEnabled()) {
            LOG.debug(getBBoxSQL);
        }

        final ResultSet result = statement.executeQuery(getBBoxSQL);

        if (!result.next()) {
            final String message = "view " + viewName + " does not deliver any records";
            LOG.error(message);
            throw new Exception(message);
        }

        featureType.setNativeBoundingBox(result.getDouble("lat_lon_xmin"),
            result.getDouble("lat_lon_ymin"),
            result.getDouble("lat_lon_xmax"),
            result.getDouble("lat_lon_ymax"),
            CRS);

        featureType.setLatLonBoundingBox(result.getDouble("lat_lon_xmin"),
            result.getDouble("lat_lon_ymin"),
            result.getDouble("lat_lon_xmax"),
            result.getDouble("lat_lon_ymax"),
            CRS);

        final GSLayerEncoder layer = new GSLayerEncoder();
        layer.setEnabled(true);
        layer.setDefaultStyle(GEOSERVER_SLD);

        LOG.info("publishing layer '" + swmmRunName + "' to geoserver " + this.restUrl);
        if (!publisher.publishDBLayer(GEOSERVER_WORKSPACE, GEOSERVER_DATASTORE, featureType, layer)) {
            final String message = "GeoServer import of swmm result '" + swmmRunName + "' was not successful";
            throw new Exception(message);
        }

        LOG.info("GeoServer import of swmm result '" + swmmRunName + "' successful");
    }
}

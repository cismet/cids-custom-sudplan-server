/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import Sirius.server.newuser.User;

import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.openide.util.lookup.ServiceProvider;

import java.sql.Connection;

import de.cismet.cids.dynamics.CidsBean;

import de.cismet.cids.trigger.AbstractDBAwareCidsTrigger;
import de.cismet.cids.trigger.CidsTrigger;
import de.cismet.cids.trigger.CidsTriggerKey;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@ServiceProvider(service = CidsTrigger.class)
public class SwmmResultViewTrigger extends AbstractDBAwareCidsTrigger {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(SwmmResultViewTrigger.class);
    public static final String CLASS = "MODELOUTPUT";
    public static final int MODEL = 12; // SWMM Model
    public static final String DOMAIN = "SUDPLAN-LINZ";

    //~ Instance fields --------------------------------------------------------

    private final CidsTriggerKey triggerKey = new CidsTriggerKey(DOMAIN, CLASS);

    //~ Methods ----------------------------------------------------------------

    @Override
    public void beforeInsert(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void afterInsert(final CidsBean cidsBean, final User user) {
        de.cismet.tools.CismetThreadPool.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        final CidsBean model = (CidsBean)cidsBean.getProperty("model");
                        final int modelId = (Integer)model.getProperty("id");

                        if (modelId == MODEL) {
                            LOG.info("new SWMM Model Result inserted, creating view for geoserver layers");
                            final ObjectMapper mapper = new ObjectMapper();
                            final ObjectNode swmmResultNode = mapper.readValue(
                                    (String)cidsBean.getProperty("ur"),
                                    ObjectNode.class);

                            final String swmmRunName = swmmResultNode.get("swmmRunName").getTextValue();
                            final int swmmRunId = swmmResultNode.get("swmmRun").getIntValue();
                            final Connection dbConnection = SwmmResultViewTrigger.this.getDbServer()
                                        .getConnectionPool()
                                        .getConnection();

                            final SwmmResultGeoserverUpdater geoserverUpdater = new SwmmResultGeoserverUpdater(
                                    dbConnection);

                            geoserverUpdater.importToGeoServer(swmmRunId, swmmRunName);
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                    "model #"
                                            + modelId
                                            + " '"
                                            + model.getProperty("name")
                                            + "' not supported by trigger");
                            }
                        }
                    } catch (Throwable t) {
                        LOG.error("could not create view for geoserver layers: " + t.getMessage(), t);
                    }
                }
            });
    }

    @Override
    public void beforeUpdate(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void afterUpdate(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void beforeDelete(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void afterDelete(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public CidsTriggerKey getTriggerKey() {
        return this.triggerKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   o  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    @Override
    public int compareTo(final CidsTrigger o) {
        return -1;
    }

    @Override
    public void afterCommittedInsert(final CidsBean cidsBean, final User user) {
    }

    @Override
    public void afterCommittedUpdate(final CidsBean cidsBean, final User user) {
    }

    @Override
    public void afterCommittedDelete(final CidsBean cidsBean, final User user) {
    }
}

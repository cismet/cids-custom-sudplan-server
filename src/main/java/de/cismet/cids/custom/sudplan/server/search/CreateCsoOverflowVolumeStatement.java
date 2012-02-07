/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.search.CidsServerSearch;

import org.apache.log4j.Logger;

import org.openide.util.Exceptions;

import java.rmi.RemoteException;

import java.util.Collection;
import java.util.Map;

/**
 * Erzeugt neue Views für WFS Layer zum Anzeigen der berechnteten Überlaufvolumina für Combinded Sever Overfloes (CSOs).
 * <br/>
 * Achtung: Funktioniert nicht, keine create & update stements in der Suche!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 * @deprecated
 */
public class CreateCsoOverflowVolumeStatement extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(CreateCsoOverflowVolumeStatement.class);

    public static final String DOMAIN = "SUDPLAN";

    public static final String CREATE_VIEW_STATEMENT_TEMPLATE = "CREATE OR REPLACE VIEW %VIEW% AS "
                + "SELECT CSO.\"name\", SWMM_RESULT.\"name\" AS \"scenario_name\", SWMM_RESULT.overflow_volume, GEOM.geo_field FROM \"public\".linz_cso CSO "
                + "JOIN \"public\".geom AS GEOM ON GEOM.id = CSO.geom AND GEOM.geo_field IS NOT NULL "
                + "JOIN \"public\".linz_swmm_scenarios AS SWMM_RUN ON CSO.id = SWMM_RUN.linz_cso_reference "
                + "JOIN \"public\".linz_swmm_result AS SWMM_RESULT ON SWMM_RESULT.id = SWMM_RUN.linz_swmm_result AND SWMM_RESULT.swmm_scenario_id = %ID% "
                + "ORDER BY CSO.name;";

    //~ Instance fields --------------------------------------------------------

    private final String createViewStatement;
    private final String swmmRunName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CreateCsoOverflowVolumeStatement object.
     *
     * @param  swmmRunId    DOCUMENT ME!
     * @param  swmmRunName  DOCUMENT ME!
     */
    public CreateCsoOverflowVolumeStatement(final int swmmRunId, final String swmmRunName) {
        this.swmmRunName = swmmRunName;
        final String viewName = "SWMM_RESULT_" + swmmRunId;

        LOG.info("creating view '" + viewName + "' for SWMM RUN '" + swmmRunName + "'");

        final String tempStatement = CREATE_VIEW_STATEMENT_TEMPLATE.replaceAll("%VIEW%", String.valueOf(viewName));
        createViewStatement = tempStatement.replaceAll("%ID%", String.valueOf(swmmRunId));

        if (LOG.isDebugEnabled()) {
            LOG.debug(createViewStatement);
        }
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final Map map = getActiveLoaclServers();
        if (map.containsKey(DOMAIN)) {
            try {
                final MetaService ms = (MetaService)map.get(DOMAIN);
                // FIXME: Server does not allow queries without results
                return ms.performCustomSearch(createViewStatement);
            } catch (RemoteException ex) {
                LOG.error("could not create view for SWMM RUN '" + swmmRunName
                            + "': " + ex.getMessage(), ex);
            }
        } else {
            LOG.error("domain '+" + DOMAIN + "' not found");
        }

        return null;
    }
}

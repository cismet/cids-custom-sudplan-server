/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaClass;
import Sirius.server.middleware.types.MetaObject;
import Sirius.server.search.CidsServerSearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Provides all uploaded emission databases with name, description and geometry.
 *
 * @version  $Revision$, $Date$
 */
public final class EmissionDatabaseSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final String DOMAIN = "SUDPLAN";                                                 // NOI18N
    private static final String CIDSCLASS = "emission_database";                                    // NOI18N
    private static final String QUERY = "SELECT id, name, description, geometry FROM " + CIDSCLASS; // NOI18N

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final MetaService metaService = (MetaService)getActiveLoaclServers().get(DOMAIN);

        if (metaService == null) {
            getLog().error("Active local server not found. Aborting search."); // NOI18N
            return null;
        }

        final int classId;
        try {
            final MetaClass metaClass = metaService.getClassByTableName(getUser(), CIDSCLASS); // NOI18N
            classId = metaClass.getID();
        } catch (final Exception ex) {
            getLog().error("Can't fetch meta class. Aborting search.", ex);                    // NOI18N
            return null;
        }

        final int[] objectIds;
        try {
            final ArrayList<ArrayList> results = metaService.performCustomSearch(QUERY);
            objectIds = new int[results.size()];

            for (int i = 0; i < results.size(); ++i) {
                final ArrayList result = results.get(i);
                objectIds[i] = (Integer)result.get(0);
            }
        } catch (final Exception e) {
            getLog().error("Can't fetch emission databases. Aborting search.", e); // NOI18N
            return null;
        }

        final Collection<MetaObject> result = new LinkedList<MetaObject>();
        try {
            for (final int objectId : objectIds) {
                final MetaObject metaObject = metaService.getMetaObject(getUser(), objectId, classId);
                result.add(metaObject);
            }
        } catch (final Exception e) {
            getLog().error("Can't create meta objects from found results. Aborting search.", e); // NOI18N
            return null;
        }

        return result;
    }
}

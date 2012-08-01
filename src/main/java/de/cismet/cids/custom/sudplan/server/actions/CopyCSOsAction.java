/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.actions;

import Sirius.server.localserver.attribute.ObjectAttribute;
import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.interfaces.domainserver.MetaServiceStore;
import Sirius.server.middleware.interfaces.domainserver.UserStore;
import Sirius.server.middleware.types.MetaClass;
import Sirius.server.middleware.types.MetaObject;
import Sirius.server.newuser.User;

import org.apache.log4j.Logger;

import java.rmi.RemoteException;

import de.cismet.cids.server.actions.ServerAction;
import de.cismet.cids.server.actions.ServerActionParameter;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class CopyCSOsAction implements ServerAction, MetaServiceStore, UserStore {

    //~ Static fields/initializers ---------------------------------------------

    public static final transient String PARAMETER_OLD_PROJECT = "oldSwmmProjectId";
    public static final transient String PARAMETER_NEW_PROJECT = "newSwmmProjectId";
    public static final transient String CSO_SERVER_ACTION = "copyCSOs";
    public static final transient String TABLENAME_CSOS = "linz_cso";
    public static final transient String SWMM_RESULT_PROPERTY = "swmm_results";
    public static final transient String SWMM_PROJECT_PROPERTY = "swmm_project";
    private static final transient Logger LOG = Logger.getLogger(CopyCSOsAction.class);

    //~ Instance fields --------------------------------------------------------

    private transient MetaService metaService;
    private transient User user;

    //~ Methods ----------------------------------------------------------------

    @Override
    public String getTaskName() {
        return CSO_SERVER_ACTION;
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        LOG.info("executing '" + CSO_SERVER_ACTION + "' server action");

        int oldSwmmProjectId = -1;
        int newSwmmProjectId = -1;
        for (final ServerActionParameter parameter : params) {
            if (PARAMETER_OLD_PROJECT.equals(parameter.getKey())) {
                try {
                    oldSwmmProjectId = Integer.valueOf(parameter.getValue());
                } catch (Throwable t) {
                    LOG.error("could, not convert value of action parameter '"
                                + PARAMETER_OLD_PROJECT + "' to int", t);
                }
            } else if (PARAMETER_NEW_PROJECT.equals(parameter.getKey())) {
                try {
                    newSwmmProjectId = Integer.valueOf(parameter.getValue());
                } catch (Throwable t) {
                    LOG.error("could, not convert value of action parameter '"
                                + PARAMETER_NEW_PROJECT + "' to int", t);
                }
            } else {
                LOG.error("unsupported server action parameter " + parameter.getKey()
                            + " = " + parameter.getValue());
            }
        }

        if ((oldSwmmProjectId < 0) || (newSwmmProjectId < 0)
                    || (this.getMetaService() == null)
                    || (this.getUser() == null)) {
            return null;
        }

        final MetaClass csoClass;
        try {
            csoClass = this.metaService.getClassByTableName(user, TABLENAME_CSOS);
        } catch (RemoteException ex) {
            LOG.error("could not retrieve class for tabblename '" + TABLENAME_CSOS + "'", ex);
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(csoClass.getID()).append(',').append(csoClass.getPrimaryKey()); // NOI18N
        sb.append(" FROM ").append(csoClass.getTableName());                                        // NOI18N
        sb.append(" WHERE swmm_project = ").append(oldSwmmProjectId);

        final MetaObject[] originalCSOs;
        if (LOG.isDebugEnabled()) {
            LOG.debug("executing SQL statement: \n" + sb);
        }
        try {
            originalCSOs = this.metaService.getMetaObject(user, sb.toString());
        } catch (RemoteException ex) {
            LOG.error("could not retrieve CSO meta objects from query '" + sb + "'", ex);
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(originalCSOs.length + " CSOs found associated to SWMM project #" + oldSwmmProjectId);
        }
        final MetaObject[] copiedCSOs = new MetaObject[originalCSOs.length];
        int i = 0;
        int successful = 0;
        for (final MetaObject originalCSO : originalCSOs) {
            final MetaObject copiedCSO = csoClass.getEmptyInstance();
            // copiedCSO.setAllClasses(originalCSO.getAllClasses());
            if (LOG.isDebugEnabled()) {
                LOG.debug("creating copy of CSO '" + originalCSO.getName() + "'");
            }

            for (final ObjectAttribute orignalAttribute : originalCSO.getAttribs()) {
                final String attributeName = orignalAttribute.getName();
                if (!("id".equals(attributeName))
                            && !(SWMM_RESULT_PROPERTY.equals(attributeName))) {
                    if (SWMM_PROJECT_PROPERTY.equals(attributeName)) {
                        orignalAttribute.setValue(newSwmmProjectId);
                    }

                    copiedCSO.addAttribute(orignalAttribute);
                }
            }

            try {
                metaService.insertMetaObject(user, copiedCSO);
                copiedCSOs[i] = copiedCSO;
                successful++;
            } catch (RemoteException ex) {
                LOG.error("could not copy CSO '" + originalCSO.getName() + "'", ex);
                copiedCSOs[i] = null;
            }

            i++;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(successful + " CSOs successfully copied");
        }
        return copiedCSOs;
    }

    @Override
    public void setMetaService(final MetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public MetaService getMetaService() {
        if (this.metaService == null) {
            LOG.error("MetaService not yet initialized!");
        }
        return this.metaService;
    }

    @Override
    public User getUser() {
        if (this.user == null) {
            LOG.error("user not yet initialized!");
        }
        return this.user;
    }

    @Override
    public void setUser(final User user) {
        this.user = user;
    }
}
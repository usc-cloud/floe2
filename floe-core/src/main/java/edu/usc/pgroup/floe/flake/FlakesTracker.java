package edu.usc.pgroup.floe.flake;

import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author kumbhare
 */
public class FlakesTracker implements PathChildrenUpdateListener {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakesTracker.class);

    /**
     * zk path for the flakes tracker.
     */
    private final String pelletTokenPath;


    /**
     * Path cache to monitor the tokens.
     */
    private ZKFlakeTokenCache flakeCache;

    /**
     * Flake update listener.
     */
    private List<FlakeUpdateListener> flakeUpdateListener;

    /**
     * Constructor.
     * @param appName name of the application.
     * @param pelletName name of the pellet whose flakes has to be tracked.
     */
    public FlakesTracker(final String appName, final String pelletName) {
        pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                appName, pelletName);
        LOGGER.debug("Listening for flake tokens for dest pellet: {} at {}",
                pelletName, pelletTokenPath);
        flakeUpdateListener = new ArrayList<>();
    }

    /**
     * add a listener for the flake updates.
     * @param listener flake update listener.
     */
    public final void addFlakeUpdateListener(
            final FlakeUpdateListener listener) {
        synchronized (this) {
            flakeUpdateListener.add(listener);
        }
    }


    /**
     * Start the tracker.
     */
    public final void start() {
        this.flakeCache = new ZKFlakeTokenCache(pelletTokenPath, this);
    }

    /**
     * Stop the falke monitor.
     */
    public final void stop() {
        this.flakeCache.stop();
    }

    /**
     * Triggered when initial list of children is cached.
     * This is retrieved synchronously.
     *
     * @param initialChildren initial list of children.
     */
    @Override
    public final void childrenListInitialized(
            final Collection<ChildData> initialChildren) {
        List<FlakeToken> flakes = new ArrayList<>();
        for (ChildData child: initialChildren) {
            FlakeToken token = (FlakeToken) Utils.deserialize(
                    child.getData());
            LOGGER.debug("Peer FID: {}", token.getFlakeID());
            flakes.add(token);
        }
        synchronized (this) {
            for (FlakeUpdateListener listener: flakeUpdateListener) {
                listener.initialFlakeList(flakes);
            }
        }
    }

    /**
     * Triggered when a new child is added.
     * Note: this is not recursive.
     *
     * @param addedChild newly added child's data.
     */
    @Override
    public final void childAdded(final ChildData addedChild) {

        String destFid = ZKPaths.getNodeFromPath(addedChild.getPath());
        LOGGER.info("Adding Peer FID: {}", destFid);

        FlakeToken token = (FlakeToken) Utils.deserialize(
                addedChild.getData());
        synchronized (this) {
            for (FlakeUpdateListener listener: flakeUpdateListener) {
                listener.flakeAdded(token);
            }
        }
    }

    /**
     * Triggered when an existing child is removed.
     * Note: this is not recursive.
     *
     * @param removedChild removed child's data.
     */
    @Override
    public final void childRemoved(final ChildData removedChild) {
        String destFid = ZKPaths.getNodeFromPath(removedChild.getPath());
        LOGGER.info("Removing Peer FID: {}", destFid);

        FlakeToken token = (FlakeToken) Utils.deserialize(
                removedChild.getData());
        synchronized (this) {
            for (FlakeUpdateListener listener: flakeUpdateListener) {
                listener.flakeRemoved(token);
            }
        }
    }

    /**
     * Triggered when a child is updated.
     * Note: This is called only when Children data is also cached in
     * addition to stat information.
     *
     * @param updatedChild update child's data.
     */
    @Override
    public final void childUpdated(final ChildData updatedChild) {
        String destFid = ZKPaths.getNodeFromPath(updatedChild.getPath());
        LOGGER.info("Updating Peer FID: {}", destFid);

        FlakeToken token = (FlakeToken) Utils.deserialize(
                updatedChild.getData());
        synchronized (this) {
            for (FlakeUpdateListener listener: flakeUpdateListener) {
                listener.flakeDataUpdated(token);
            }
        }
    }

    /**
     * @return rebuilds the cache if it is old and returns the list of current
     * flakes.
     */
    public final List<FlakeToken> getCurrentFlakes() {
        flakeCache.rebuild();
        List<ChildData> children = flakeCache.getCurrentCachedData();
        List<FlakeToken> flakes = new ArrayList<>();
        for (ChildData child: children) {
            FlakeToken token = (FlakeToken) Utils.deserialize(
                    child.getData());
            LOGGER.debug("FID: {}", token.getFlakeID());
            flakes.add(token);
        }
        return flakes;
    }

}

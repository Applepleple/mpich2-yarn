/**
 *
 */
package org.apache.hadoop.yarn.mpi.server.handler;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;

public class MPIAMRMAsyncHandler implements CallbackHandler {
  private static final Log LOG = LogFactory.getLog(MPIAMRMAsyncHandler.class);
  private final Map<String, List<Container>> hostToContainers = new HashMap<>();
  private final List<Container> distinctContainers = new ArrayList<>();
  public final List<Container> acquiredContainers = new ArrayList<>();
  private final AtomicInteger acquiredContainersCount = new AtomicInteger(0);
  private final AtomicInteger neededContainersCount = new AtomicInteger();
  private List<String> hostBlackList = new ArrayList<>();
  private AMRMClientAsync<AMRMClient.ContainerRequest> rmClientAsync;


  public void setRmClientAsync(AMRMClientAsync<AMRMClient.ContainerRequest> rmClientAsync) {
    this.rmClientAsync = rmClientAsync;
  }

  public int getAllocatedContainerNumber() {
    return acquiredContainersCount.get();
  }

  /**
   * @param count
   *          how many containers do we need?
   */
  public void setNeededContainersCount(int count) {
    neededContainersCount.set(count);
  }

  public List<Container> getDistinctContainers() {
    return new ArrayList<>(distinctContainers);
  }

  public List<Container> getAcquiredContainers() {
    return new ArrayList<>(acquiredContainers);
  }

  public Map<String, List<Container>> getHostToContainer() {
    return hostToContainers;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #onContainersCompleted(java.util.List)
   */
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
//    rmClientAsync.updateBlacklist(null, hostBlackList);
    hostBlackList.clear();
    for (ContainerStatus status : statuses) {
      LOG.info("CompletedContainer: Id=" + status.getContainerId());
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #onContainersAllocated(java.util.List)
   */
  @Override
  public void onContainersAllocated(List<Container> containers) {
    for (Container acquiredContainer : containers) {
      String msg = "AcquiredContainer: Id=" + acquiredContainer.getId()
          + ", NodeId=" + acquiredContainer.getNodeId() + ", Host="
          + acquiredContainer.getNodeId().getHost() + ", Resource="
          + acquiredContainer.getResource();
      LOG.info(msg);
      acquiredContainers.add(acquiredContainer);
      String host = acquiredContainer.getNodeId().getHost();
      if (!hostToContainers.containsKey(host)) {
        List<Container> hostContainers = new ArrayList<>();
        hostContainers.add(acquiredContainer);
        hostToContainers.put(host, hostContainers);

        distinctContainers.add(acquiredContainer);
      } else {
        hostToContainers.get(host).add(acquiredContainer);
      }
      if (hostBlackList.isEmpty()) {
        LOG.info("First resource scheduling time: " + System.currentTimeMillis());
      }
      hostBlackList.add(host);
      rmClientAsync.updateBlacklist(hostBlackList, null);
    }
    acquiredContainersCount.addAndGet(containers.size());
    LOG.info("Current=" + acquiredContainersCount.get() + ", Needed="
        + neededContainersCount.get());
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #onShutdownRequest()
   */
  @Override
  public void onShutdownRequest() {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #onNodesUpdated(java.util.List)
   */
  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #getProgress()
   */
  @Override
  public float getProgress() {
    float neededTotal = neededContainersCount.get();
    float acquiredTotal = acquiredContainersCount.get();
    if (neededTotal == 0) {
      return 0.0f;
    } else {
      return acquiredTotal / neededTotal;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
   * #onError(java.lang.Throwable)
   */
  @Override
  public void onError(Throwable e) {
    // TODO Auto-generated method stub

  }

}

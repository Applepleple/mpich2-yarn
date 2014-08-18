/**
 *
 */
package org.apache.hadoop.yarn.mpi.server.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;

/**
 * @author �Ჩ��
 *
 */
public class MPIAMRMAsyncHandler implements CallbackHandler {
  private static final Log LOG = LogFactory.getLog(MPIAMRMAsyncHandler.class);
  private final Map<String, Integer> hostToProcNum = new HashMap<String, Integer>();
  private final Map<String, Container> hostToContainer = new HashMap<String, Container>();
  private final List<Container> distinctContainers = new ArrayList<Container>();
  public final Set<ContainerId> acquiredContainers = new HashSet<ContainerId>();
  private final AtomicInteger acquiredContainersCount = new AtomicInteger(0);

  public int getAllocatedContainerNumber() {
    return acquiredContainersCount.get();
  }

  public Map<String, Integer> getHostToProcNum() {
    return new HashMap<String, Integer>(hostToProcNum);
  }

  public List<Container> getDistinctContainers() {
    return new ArrayList<Container>(distinctContainers);
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
      LOG.info("AcquiredContainer: Id=" + acquiredContainer.getId()
          + ", NodeId=" + acquiredContainer.getNodeId() + ", Host="
          + acquiredContainer.getNodeId().getHost());
      acquiredContainers.add(acquiredContainer.getId());
      String host = acquiredContainer.getNodeId().getHost();
      if (!hostToContainer.containsKey(host)) {
        hostToContainer.put(host, acquiredContainer);
        hostToProcNum.put(host, new Integer(1));
        distinctContainers.add(acquiredContainer);
      } else {
        int procNum = hostToProcNum.get(host).intValue();
        procNum++;
        hostToProcNum.put(host, new Integer(procNum));
        // TODO check if this works
        // Container container = hostToContainer.get(host);
        // allocatedContainer.setState(ContainerState.COMPLETE);
      }
    }
    acquiredContainersCount.addAndGet(containers.size());
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
    // TODO Auto-generated method stub
    return 0;
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

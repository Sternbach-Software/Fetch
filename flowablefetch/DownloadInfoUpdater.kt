package flowablefetch

import com.tonyodev.fetch2.database.DownloadInfo
import com.tonyodev.fetch2.database.FetchDatabaseManagerWrapper


class DownloadInfoUpdater(private val fetchDatabaseManagerWrapper: FlowableFetchDatabaseManagerWrapper) {

    fun updateFileBytesInfoAndStatusOnly(downloadInfo: DownloadInfo) {
        fetchDatabaseManagerWrapper.updateFileBytesInfoAndStatusOnly(downloadInfo)
    }

    fun update(downloadInfo: DownloadInfo) {
        fetchDatabaseManagerWrapper.update(downloadInfo)
    }

    fun getNewDownloadInfoInstance(): DownloadInfo {
        return fetchDatabaseManagerWrapper.getNewDownloadInfoInstance()
    }

}

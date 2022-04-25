package flowablefetch

import android.annotation.SuppressLint
import android.content.Context
import com.tonyodev.fetch2.Download
import com.tonyodev.fetch2.Fetch
import com.tonyodev.fetch2.FetchConfiguration
import com.tonyodev.fetch2.exception.FetchException
import com.tonyodev.fetch2.fetch.FetchImpl
import com.tonyodev.fetch2.fetch.FetchModulesBuilder
import com.tonyodev.fetch2core.GLOBAL_FETCH_CONFIGURATION_NOT_SET
import kotlinx.coroutines.flow.Flow

interface FlowableFetch: Fetch {
    fun getDownloadsFlow(): Flow<List<Download>>
    companion object Impl {

        private val lock = Any()
        @SuppressLint("StaticFieldLeak")
        @Volatile
        private var defaultFetchConfiguration: FlowableFetchConfiguration? = null
        @Volatile
        private var defaultFetchInstance: Fetch? = null
/*
        *//**
         * Sets the default Configuration settings on the default Fetch instance.
         * @param fetchConfiguration custom Fetch Configuration
         * *//*
        fun setDefaultInstanceConfiguration(fetchConfiguration: FetchConfiguration) {
            synchronized(lock) {
                defaultFetchConfiguration = fetchConfiguration
            }
        }

        *//**
         * Get the default Fetch Configuration set with setDefaultInstanceConfiguration(fetchConfiguration: FetchConfiguration)
         * or setDefaultInstanceConfiguration(context: Context)
         * @return default FetchConfiguration
         * *//*
        fun getDefaultFetchConfiguration(): FetchConfiguration? {
            return synchronized(lock) {
                defaultFetchConfiguration
            }
        }

        *//**
         * @throws FetchException if default FetchConfiguration is not set.
         * @return Get default Fetch instance
         * *//*
        fun getDefaultInstance(): Fetch {
            return synchronized(lock) {
                val fetchConfiguration = defaultFetchConfiguration
                    ?: throw FetchException(GLOBAL_FETCH_CONFIGURATION_NOT_SET)
                val defaultFetch = defaultFetchInstance
                if (defaultFetch == null || defaultFetch.isClosed) {
                    val newDefaultFetch = FetchImpl.newInstance(FetchModulesBuilder.buildModulesFromPrefs(fetchConfiguration))
                    defaultFetchInstance = newDefaultFetch
                    newDefaultFetch
                } else {
                    defaultFetch
                }
            }
        }*/

        /**
         * Creates a custom Instance of Fetch with the given configuration and namespace.
         * @param fetchConfiguration custom Fetch Configuration
         * @return custom Fetch instance
         * */
        fun getInstance(fetchConfiguration: FlowableFetchConfiguration, createFetchConfiguration: () -> FetchConfiguration): FlowableFetch {
            return FlowableFetchImpl.newInstance(flowablefetch.FetchModulesBuilder.buildModulesFromPrefs(fetchConfiguration), createFetchConfiguration)
        }

    }
}

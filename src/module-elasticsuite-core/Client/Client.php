<?php
/**
 * DISCLAIMER :
 *
 * Do not edit or add to this file if you wish to upgrade Smile ElasticSuite to newer
 * versions in the future.
 *
 * @category  Smile_Elasticsuite
 * @package   Smile\ElasticsuiteCore
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2018 Smile
 * @license   Open Software License ("OSL") v. 3.0
 */

namespace Smile\ElasticsuiteCore\Client;

use Smile\ElasticsuiteCore\Api\Client\ClientConfigurationInterface;
use Smile\ElasticsuiteCore\Api\Client\ClientInterface;

/**
 * ElasticSearch client implementation.
 *
 * @category  Smile
 * @package   Smile\ElasticsuiteCore
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 *
 * @SuppressWarnings(TooManyPublicMethods)
 */
class Client implements ClientInterface
{
    /**
     * @var \Elasticsearch\Client
     */
    private $esClient;

    /**
     * @var array
     */
    protected $timeoutParams;

    /**
     * Constructor.
     *
     * @param ClientConfigurationInterface $clientConfiguration Client configuration factory.
     * @param ClientBuilder                $clientBuilder       ES client builder.
     */
    public function __construct(ClientConfigurationInterface $clientConfiguration, ClientBuilder $clientBuilder)
    {
        $this->esClient = $clientBuilder->build($clientConfiguration->getOptions());

        $this->timeoutParams = [
            'client' => [
                'timeout' => $clientConfiguration->getTimeout(),
                'server_timeout' => $clientConfiguration->getConnectionTimeout(),
            ],
        ];
    }

    /**
     * {@inheritDoc}
     */
    public function info()
    {
        return $this->esClient->info($this->prepareParams());
    }

    /**
     * {@inheritDoc}
     */
    public function ping()
    {
        return $this->esClient->ping($this->prepareParams());
    }

    /**
     * {@inheritDoc}
     */
    public function createIndex($indexName, $indexSettings)
    {
        $this->esClient->indices()->create($this->prepareParams(['index' => $indexName, 'body' => $indexSettings]));
    }

    /**
     * {@inheritDoc}
     */
    public function deleteIndex($indexName)
    {
        $this->esClient->indices()->delete($this->prepareParams(['index' => $indexName]));
    }

    /**
     * {@inheritDoc}
     */
    public function indexExists($indexName)
    {
        return $this->esClient->indices()->exists($this->prepareParams(['index' => $indexName]));
    }

    /**
     * {@inheritDoc}
     */
    public function putIndexSettings($indexName, $indexSettings)
    {
        $this->esClient->indices()->putSettings($this->prepareParams(['index' => $indexName, 'body' => $indexSettings]));
    }

    /**
     * {@inheritDoc}
     */
    public function putMapping($indexName, $type, $mapping)
    {
        $this->esClient->indices()->putMapping($this->prepareParams(['index' => $indexName, 'type'  => $type, 'body'  => [$type => $mapping]]));
    }

    /**
     * {@inheritDoc}
     */
    public function forceMerge($indexName)
    {
        $this->esClient->indices()->forceMerge($this->prepareParams(['index' => $indexName]));
    }

    /**
     * {@inheritDoc}
     */
    public function refreshIndex($indexName)
    {
        $this->esClient->indices()->refresh($this->prepareParams(['index' => $indexName]));
    }

    /**
     * {@inheritDoc}
     */
    public function getIndicesNameByAlias($indexAlias)
    {
        $indices = [];
        try {
            $indices = $this->esClient->indices()->getMapping($this->prepareParams(['index' => $indexAlias]));
        } catch (\Elasticsearch\Common\Exceptions\Missing404Exception $e) {
            ;
        }

        return array_keys($indices);
    }

    /**
     * {@inheritDoc}
     */
    public function updateAliases($aliasActions)
    {
        $this->esClient->indices()->updateAliases($this->prepareParams(['body' => ['actions' => $aliasActions]]));
    }

    /**
     * {@inheritDoc}
     */
    public function bulk($bulkParams)
    {
        return $this->esClient->bulk($this->prepareParams($bulkParams));
    }

    /**
     * {@inheritDoc}
     */
    public function search($params)
    {
        return $this->esClient->search($this->prepareParams($params));
    }

    /**
     * {@inheritDoc}
     */
    public function analyze($params)
    {
        return $this->esClient->indices()->analyze($this->prepareParams($params));
    }

    /**
     * {@inheritDoc}
     */
    public function indexStats($indexName)
    {
        return $this->esClient->indices()->stats($this->prepareParams(['index' => $indexName]));
    }

    /**
     * {@inheritDoc}
     */
    public function termvectors($params)
    {
        return $this->esClient->termvectors($this->prepareParams($params));
    }

    /**
     * @param array $params
     * @return array
     */
    protected function prepareParams($params = [])
    {
        return array_merge(
            $params,
            $this->timeoutParams
        );
    }
}

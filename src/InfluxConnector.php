<?php

namespace Lkt\Connectors;

use InfluxDB2\Client;
use InfluxDB2\Model\Organization;
use InfluxDB2\Model\PostBucketRequest;
use InfluxDB2\Model\WritePrecision;
use InfluxDB2\Service\BucketsService;
use InfluxDB2\Service\OrganizationsService;
use Lkt\Connectors\Cache\QueryCache;
use Lkt\Factory\Schemas\ComputedFields\AbstractComputedField;
use Lkt\Factory\Schemas\Fields\AbstractField;
use Lkt\Factory\Schemas\Fields\JSONField;
use Lkt\Factory\Schemas\Fields\PivotField;
use Lkt\Factory\Schemas\Fields\RelatedField;
use Lkt\Factory\Schemas\Fields\RelatedKeysField;
use Lkt\Factory\Schemas\Schema;
use Lkt\QueryBuilding\Query;
use function Lkt\Tools\Parse\clearInput;

class InfluxConnector extends DatabaseConnector
{
    protected int $port = 8086;
    protected string $charset = 'utf8';
    protected string $token = '';
    protected string $organization = '';
    protected string $bucket = '';

    protected string $rememberTotal = '';

    protected Client|null $client = null;

    protected array $cachedBuckedExists = [];
    protected array $cachedOrgExists = [];


    /** @var InfluxConnector[] */
    protected static array $connectors = [];

    public static function define(string $name): static
    {
        $r = new static($name);
        DatabaseConnections::set($r);
        static::$connectors[$name] = $r;
        return $r;
    }

    public static function get(string $name): ?static
    {
        if (!isset(static::$connectors[$name])) {
            throw new \Exception("Connector '{$name}' doesn't exists");
        }
        return static::$connectors[$name];
    }

    public function setRememberTotal(string $rememberTotal): InfluxConnector
    {
        $this->rememberTotal = $rememberTotal;
        return $this;
    }

    public function connect(): static
    {
        if ($this->client !== null) return $this;

        // Perform the connection
        try {
            $this->client = new Client([
                'url' => "$this->host:{$this->port}",
                'token' => $this->token,
                'org' => $this->organization,
                'bucket' => $this->bucket,
                'precision' => WritePrecision::S,
            ]);
        } catch (\Exception $e) {
            die ('Connection to database failed');
        }
        return $this;
    }

    public function disconnect(): static
    {
        $this->client = null;
        return $this;
    }

    public function write(array $data, string $measurement = 'point')
    {
        $this->connect();

        if (!$this->bucketExists($this->bucket)) {
            $this->createBucket($this->bucket);
        }

        if (!$measurement) $measurement = 'point';

        $points = array_map(function (&$row) use ($measurement) {

            $payload = [$measurement];
            $seconds = isset($row['time']) ? $row['time'] : microtime(true);
            unset($row['time']);

            if (isset($row['tags']) and is_array($row['tags'])) {
                $tags = [];
                foreach ($row['tags'] as $tag => $v) $tags[] = "$tag=$v";
                $payload[0] .= ','.implode(',', $tags);
                unset($row['tags']);
            }

            $fields = [];
            foreach ($row as $key => $datum) {
                if (is_string($datum)) {
                    $d = clearInput($datum);
                    $fields[] = "$key=\"$d\"";
                } else {
                    $fields[] = "$key=$datum";
                }
            }

            if (count($fields) > 0) {
                $payload[] = implode(',', $fields);
            }

            $payload[] = $seconds;
            return implode(' ', $payload);
        }, $data);

        $writeApi = $this->client->createWriteApi();
        $writeApi->write($points, WritePrecision::S, $this->bucket, $this->organization);
    }

    public function createBucket(string $name): bool
    {
        $this->connect();
        $bucketsService = $this->client->createService(BucketsService::class);

        $bucketRequest = new PostBucketRequest();
        $bucketRequest->setName($name)->setOrgId($this->findMyOrg()?->getId());

        //create bucket
        $bucketsService->postBuckets($bucketRequest);
        $this->cachedBuckedExists[$name] = true;
        return true;
    }

    public function findMyOrg(): ?Organization
    {
        if (isset($this->cachedOrgExists[$this->client->options["org"]])) return $this->cachedOrgExists[$this->client->options["org"]];
        $this->connect();
        /** @var OrganizationsService $orgService */
        $orgService = $this->client->createService(OrganizationsService::class);
        $orgs = $orgService->getOrgs()->getOrgs();
        foreach ($orgs as $org) {
            if ($org->getName() == $this->client->options["org"]) {
                $this->cachedOrgExists[$this->client->options["org"]] = $org;
                return $org;
            }
        }
        $this->cachedOrgExists[$this->client->options["org"]] = null;
        return null;
    }

    public function bucketExists(string $bucket): bool
    {
        if (isset($this->cachedBuckedExists[$bucket])) return $this->cachedBuckedExists[$bucket];

        $this->connect();
        /** @var BucketsService $orgService */
        $orgService = $this->client->createService(BucketsService::class);
        $existingBuckets = $orgService->getBuckets()->getBuckets();
        foreach ($existingBuckets as $existingBucket) {
            if ($existingBucket->getName() == $bucket) {
                $this->cachedBuckedExists[$bucket] = true;
                return true;
            }
        }
        $this->cachedBuckedExists[$bucket] = false;
        return false;
    }

    public function read(array $filter = [], ?string $start = null, ?string $last = null, ?string $measurement = null): array
    {
        $this->connect();

        if (!$this->bucketExists($this->bucket)) {
            $this->createBucket($this->bucket);
        }

        $range = [];
        if (!$start) $start = '1970-01-01T00:00:00.000000001Z';
        $start = trim($start);
        $last = trim($last);
        if ($start !== '') $range[] = "start: $start";
        if ($last !== '') $range[] = "stop: $last";

        $rangeStr = implode(' , ', $range);
        $auxQuery = ["from(bucket: \"{$this->bucket}\")"];

        if ($rangeStr !== '') $auxQuery[] = "range({$rangeStr})";
        if (count($filter) > 0) foreach ($filter as $value) $auxQuery[] = $value;

        if ($measurement) $auxQuery[] = 'filter(fn: (r) => r["_measurement"] == "'.$measurement.'")';

        $query = implode(' |> ', $auxQuery);

        if (!$this->forceRefresh && !$this->ignoreCache && QueryCache::isset($this->name, $query)) {
            return QueryCache::get($this->name, $query)->getLatestResults();
        }

        $tables = $this->client->createQueryApi()->query($query, $this->organization);

        $r = [];

        foreach ($tables as $table) {
            foreach ($table->records as $record) {
                $field = $record->getField();
                $value = $record->getValue();
                try {
                    $time = $record->getTime();
                    if (!isset($r[$time])) $r[$time] = ['time' => $time];
                    $r[$time][$field] = $value;
                } catch (\RuntimeException $exception) {
                    $r[0][$field] = $value;
                }
            }
        }

        $r = array_values($r);
        QueryCache::set($this->name, $query, $r);
        return $r;
    }

    public function first(array $filter = [], string $start = '1970-01-01T00:00:00.000000001Z', ?string $last = null): array
    {
        $filter[] = 'first()';
        $r = $this->read($filter, $start, $last);
        if (count($r) > 0) return $r[0];
        return [];
    }

    public function last(array $filter = [], string $start = '1970-01-01T00:00:00.000000001Z', ?string $last = null): array
    {
        $filter[] = 'last()';
        $r = $this->read($filter, $start, $last);
        if (count($r) > 0) return $r[0];
        return [];
    }

    public function query(string $query, array $replacements = []): ?array
    {
        return [];
    }

    public function extractSchemaColumns(Schema $schema): array
    {
        $table = $schema->getTable();

        /** @var AbstractField[] $fields */
        $fields = $schema->getSameTableFields();

        $r = [];

        foreach ($fields as $key => $field) {
            if ($field instanceof PivotField || $field instanceof RelatedField || $field instanceof RelatedKeysField || $field instanceof AbstractComputedField) {
                continue;
            }
            $column = trim($field->getColumn());
            if ($field instanceof JSONField && $field->isCompressed()) {
                $r[] = "UNCOMPRESS({$table}.{$column}) as {$key}";
            } else {
                $r[] = "{$table}.{$column} as {$key}";
            }
        }

        return $r;
    }

    private function buildColumns(Query $builder): string
    {
        $r = [];
        $table = $builder->getTableNameOrAlias();
        foreach ($builder->getColumns() as $column) {
            $r[] = $this->buildColumnString($column, $table);
        }

        return implode(',', $r);
    }


    private function buildColumnString(string $column, string $table): string
    {
        $prependTable = $table !== '' ? "{$table}." : '';
        $tempColumn = str_replace([' as ', ' AS ', ' aS ', ' As '], '{{---LKT_SEPARATOR---}}', $column);
        $exploded = explode('{{---LKT_SEPARATOR---}}', $tempColumn);

        $key = trim($exploded[0]);
        $alias = isset($exploded[1]) ? trim($exploded[1]) : '';

        if (str_starts_with($column, 'UNCOMPRESS') || str_starts_with($column, "'") || str_starts_with($column, "DISTINCT") || strpos($column, '(') > 0) {
            if ($alias !== '') {
                $r = "{$key} AS {$alias}";
            } else {
                $r = $key;
            }
        } elseif (strpos($key, $prependTable) === 0) {
            if ($alias !== '') {
                $r = "{$key} AS {$alias}";
            } else {
                $r = $key;
            }
        } else {
            if ($alias !== '') {
                $r = "{$prependTable}{$key} AS {$alias}";
            } else {
                $r = "{$prependTable}{$key}";
            }
        }

        return $r;
    }

    public function makeUpdateParams(array $params = [], string $type = 'create'): string
    {
        $r = [];
        foreach ($params as $field => $value) {
            $v = addslashes(stripslashes($value));
            if (strpos($value, 'COMPRESS(') === 0) {
                $r[] = "`{$field}`={$value}";
            } else {
                $r[] = "`{$field}`='{$v}'";
            }
        }
        return trim(implode(',', $r));
    }

    public function getLastInsertedId(): int
    {
        if ($this->connection === null) {
            return 0;
        }
        return (int)$this->connection->lastInsertId();
    }

    public function getQuery(Query $builder, string $type, string $countableField = null): string
    {
        return '';
    }

    public function prepareDataToStore(Schema $schema, array $data): array
    {
        return [];
    }

    public function setToken(string $token): static
    {
        $this->token = $token;
        return $this;
    }

    public function setOrganization(string $organization): static
    {
        $this->organization = $organization;
        return $this;
    }

    public function setBucket(string $bucket): static
    {
        $this->bucket = $bucket;
        return $this;
    }
}
<?php

namespace Lkt\DatabaseConnectors;

use InfluxDB2\Client;
use InfluxDB2\Model\Organization;
use InfluxDB2\Model\PostBucketRequest;
use InfluxDB2\Model\WritePrecision;
use InfluxDB2\Point;
use InfluxDB2\Service\BucketsService;
use InfluxDB2\Service\OrganizationsService;
use Lkt\DatabaseConnectors\Cache\QueryCache;
use Lkt\Factory\Schemas\ComputedFields\AbstractComputedField;
use Lkt\Factory\Schemas\Fields\AbstractField;
use Lkt\Factory\Schemas\Fields\BooleanField;
use Lkt\Factory\Schemas\Fields\DateTimeField;
use Lkt\Factory\Schemas\Fields\EmailField;
use Lkt\Factory\Schemas\Fields\FileField;
use Lkt\Factory\Schemas\Fields\FloatField;
use Lkt\Factory\Schemas\Fields\ForeignKeyField;
use Lkt\Factory\Schemas\Fields\HTMLField;
use Lkt\Factory\Schemas\Fields\IntegerField;
use Lkt\Factory\Schemas\Fields\JSONField;
use Lkt\Factory\Schemas\Fields\PivotField;
use Lkt\Factory\Schemas\Fields\RelatedField;
use Lkt\Factory\Schemas\Fields\RelatedKeysField;
use Lkt\Factory\Schemas\Fields\StringField;
use Lkt\Factory\Schemas\Fields\UnixTimeStampField;
use Lkt\Factory\Schemas\Schema;
use Lkt\QueryBuilding\Query;

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

    public function setRememberTotal(string $rememberTotal): InfluxConnector
    {
        $this->rememberTotal = $rememberTotal;
        return $this;
    }

    public function connect(): DatabaseConnector
    {
        if ($this->client !== null) {
            return $this;
        }

        // Perform the connection
        try {

            $this->client = new Client([
                'url' => "$this->host:{$this->port}",
                'token' => $this->token,
                'org' => $this->organization,
                'bucket' => $this->bucket,
                'precision' => WritePrecision::S,
            ]);


//
//            dump($client);
//
//            $queryApi = $client->createQueryApi();
//            dump($queryApi);
////            $result = $queryApi->query("SHOW DATABASES");
//
//
//            $writeApi = $client->createWriteApi();
//
//            $data = "mem,host=host1 used_percent=23.43234543";
//
//            $point = Point::measurement('point')
//                ->addField('v1', 23.43234543)
//                ->addField('v2', 33.43234543)
//                ->addField('v3', 43.43234543)
//                ->addField('v4', 53.43234543)
//                ->addField('real', 77.43234543)
//                ->time(microtime(true));
//
////            dd($point);
//
//            $writeApi->write($point, WritePrecision::S, $this->bucket, $this->organization);
//
//
//
//            dd(1);
////            $query = "from(bucket: \"$this->bucket\") |> range(start: -1d)";
//            $result = $queryApi->query($query);
//            dd($result);


//            $dsn = sprintf("influxdb://{$this->user}:{$this->password}@%s:%s/%s", $this->host, $this->port, $this->database);
//            $this->connectedDb = Client::fromDSN($dsn);
        } catch (\Exception $e) {
            die ('Connection to database failed');
        }
        return $this;
    }

    public function disconnect(): DatabaseConnector
    {
        $this->client = null;
        return $this;
    }

    public function write(array $data)
    {
        $this->connect();

        if (!$this->bucketExists($this->bucket)) {
            $this->createBucket($this->bucket);
        }

        $points = array_map(function ($row) {
            $point = Point::measurement('point');
            $timeIncluded = false;
            foreach ($row as $key => $datum) {
                if ($key === 'time') {
                    $point->time($datum);
                    $timeIncluded = true;
                } elseif ($key === 'tags') {
                    foreach ($datum as $tag => $v) $point->addTag($tag, $v);
                } else {
                    $point->addField($key, $datum);
                }
            }

            if (!$timeIncluded) $point->time(microtime(true));
            return $point;
        }, $data);

        $writeApi = $this->client->createWriteApi();
//        foreach ($points as $point) {
//            $writeApi->write($point, WritePrecision::S, $this->bucket, $this->organization);
//        }
        $writeApi->write($points, WritePrecision::S, $this->bucket, $this->organization);

    }

    public function createBucket(string $name): bool
    {
        $this->connect();
        $bucketsService = $this->client->createService(BucketsService::class);

//        $rule = new BucketRetentionRules();
//        $rule->setEverySeconds(3600);

        $bucketRequest = new PostBucketRequest();
        $bucketRequest->setName($name)
//            ->setRetentionRules([$rule])
            ->setOrgId($this->findMyOrg()->getId());

        //create bucket
        $respBucket = $bucketsService->postBuckets($bucketRequest);
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

    public function read(array $filter = [], string $start = '1970-01-01T00:00:00.000000001Z', ?string $last = null): array
    {
        $this->connect();

        if (!$this->bucketExists($this->bucket)) {
            $this->createBucket($this->bucket);
        }

        $range = [];
        $start = trim($start);
        $last = trim($last);
        if ($start !== '') $range[] = "start: $start";
        if ($last !== '') $range[] = "stop: $last";

        $rangeStr = implode(' , ', $range);
        $auxQuery = ["from(bucket: \"{$this->bucket}\")"];

        if ($rangeStr !== '') $auxQuery[] = "range({$rangeStr})";
        if (count($filter) > 0) foreach ($filter as $value) $auxQuery[] = $value;

        $query = implode(' |> ', $auxQuery);

        if (!$this->forceRefresh && !$this->ignoreCache && QueryCache::isset($this->name, $query)) {
            return QueryCache::get($this->name, $query)->getLatestResults();
        }

        $tables = $this->client->createQueryApi()->query($query, $this->organization);

        $r = [];

        foreach ($tables as $table) {
            foreach ($table->records as $record) {
                $time = $record->getTime();
                $field = $record->getField();
                $value = $record->getValue();
                if (!isset($r[$time])) $r[$time] = ['time' => $time];
                $r[$time][$field] = $value;
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

    public function makeUpdateParams(array $params = []): string
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
        $whereString = $builder->getQueryWhere();

        switch ($type) {
            case 'select':
            case 'selectDistinct':
            case 'count':
                $from = [];
                foreach ($builder->getJoins() as $join) {
                    $from[] = (string)$join;
                }

                $joinedWhere = [];
                $joinedBuilders = $builder->getJoinedBuilders();
                if (count($joinedBuilders) > 0) {
                    foreach ($joinedBuilders as $key => $joinedBuilder) {
                        $joinData = $builder->getJoinedBuildersRelation($key);
                        $from[] = $joinedBuilder->getJoinString($joinData[0], $joinData[1], $builder->formatJoinedColumn($joinData[2]));
                    }
                }
                $fromString = implode(' ', $from);
                $fromString = str_replace('{{---LKT_PARENT_TABLE---}}', $builder->getTable(), $fromString);

                if (count($joinedWhere) > 0) {
                    $whereString = implode(' AND ', [$whereString, implode(' AND ', $joinedWhere)]);
                }

                $distinct = '';

                if ($type === 'selectDistinct') {
                    $distinct = 'DISTINCT';
                    $type = 'select';
                } elseif ($type === 'count') {
                    $distinct = 'DISTINCT';
                }

                $tableAlias = $builder->getTableAlias();
                $asTableAlias = $builder->hasTableAlias() ? " AS {$tableAlias} " : '';

                if ($type === 'select') {
                    $columns = $this->buildColumns($builder);
                    $orderBy = '';
                    $pagination = '';

                    if ($builder->hasOrder()) {
                        $orderBy = " ORDER BY {$builder->getOrder()}";
                    }

                    if ($builder->hasPagination()) {
                        $p = $builder->getPage() * $builder->getLimit();
                        $pagination = " LIMIT {$p}, {$builder->getLimit()}";

                    } elseif ($builder->hasLimit()) {
                        $pagination = " LIMIT {$builder->getLimit()}";
                    }


                    $r = "SELECT {$distinct} {$columns} FROM {$builder->getTable()}{$asTableAlias} {$fromString} WHERE 1 {$whereString} {$orderBy} {$pagination}";
                    $r = str_replace('DISTINCT DISTINCT', 'DISTINCT', $r);
                    return $r;
                }

                if ($type === 'count') {
                    return "SELECT COUNT({$distinct} {$countableField}) AS Count FROM {$builder->getTable()}{$asTableAlias} {$fromString} WHERE 1 {$whereString}";
                }
                return '';

            case 'update':
            case 'insert':
                $data = $this->makeUpdateParams($builder->getData());

                if ($type === 'update') {
                    return "UPDATE {$builder->getTable()} SET {$data} WHERE 1 {$whereString}";
                }

                if ($type === 'insert') {
                    return "INSERT INTO {$builder->getTable()} SET {$data}";
                }
                return '';

            case 'delete':
                return "DELETE FROM {$builder->getTable()} WHERE 1 {$whereString}";

            default:
                return '';
        }
    }

    public function prepareDataToStore(Schema $schema, array $data): array
    {
        $fields = $schema->getAllFields();
        $parsed = [];

        foreach ($fields as $column => $field) {
            $columnKey = $column;
            if ($field instanceof ForeignKeyField) {
                $columnKey .= 'Id';
            }
            if (array_key_exists($columnKey, $data)) {
                $value = $data[$columnKey];

                $compress = $field instanceof JSONField && $field->isCompressed();

                if ($field instanceof StringField
                    || $field instanceof EmailField
                    || $field instanceof RelatedKeysField
                    || $field instanceof ForeignKeyField
                ) {
                    $r = trim($value);
                    if ($compress) {
                        $value = "COMPRESS('{$r}')";
                    } else {
                        $value = $r;
                    }
                }

                if ($field instanceof HTMLField) {
                    $r = $this->escapeDatabaseCharacters($value);
                    if ($compress) {
                        $value = "COMPRESS('{$r}')";
                    } else {
                        $value = $r;
                    }
                }

                if ($field instanceof BooleanField) {
                    $value = $value === true ? 1 : 0;
                }

                if ($field instanceof IntegerField) {
                    $value = (int)$value;
                }

                if ($field instanceof FloatField) {
                    $value = (float)$value;
                }

                if ($field instanceof UnixTimeStampField) {
                    if ($value instanceof \DateTime) {
                        $value = strtotime($value->format('Y-m-d H:i:s'));
                    } else {
                        $value = 0;
                    }
                }

                if ($field instanceof DateTimeField) {
                    if ($value instanceof \DateTime) {
                        $value = $value->format('Y-m-d H:i:s');
                    } else {
                        $value = '0000-00-00 00:00:00';
                    }
                }

                if ($field instanceof FileField) {
                    if (is_object($value)) {
                        $value = $value->name;
                    } else {
                        $value = '';
                    }
                }

                if ($field instanceof JSONField) {
                    if (is_array($value)) {
                        $v = htmlspecialchars(json_encode($value), JSON_UNESCAPED_UNICODE | ENT_QUOTES, 'UTF-8');
                        $v = $this->escapeDatabaseCharacters($v);

                        if ($compress) {
                            $v = "COMPRESS('{$v}')";
                        }
                        $value = $v;
                    }
                }

                $parsed[$field->getColumn()] = $value;
            }
        }

        return $parsed;
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
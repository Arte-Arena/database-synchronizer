package main

import (
	"context"
	"database/sql"
	"database_sync/database"
	"database_sync/utils"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type OrderStatus string

const (
	StatusPendente     OrderStatus = "Pendente"
	StatusEmAndamento  OrderStatus = "Em andamento"
	StatusArteOK       OrderStatus = "Arte OK"
	StatusEmEspera     OrderStatus = "Em espera"
	StatusCorTeste     OrderStatus = "Cor teste"
	StatusProcessando  OrderStatus = "Processando"
	StatusImpresso     OrderStatus = "Impresso"
	StatusEmImpressao  OrderStatus = "Em impressão"
	StatusSeparacao    OrderStatus = "Separação"
	StatusCosturado    OrderStatus = "Costurado"
	StatusPrensa       OrderStatus = "Prensa"
	StatusCalandra     OrderStatus = "Calandra"
	StatusEmSeparacao  OrderStatus = "Em separação"
	StatusRetirada     OrderStatus = "Retirada"
	StatusEmEntrega    OrderStatus = "Em entrega"
	StatusEntregue     OrderStatus = "Entregue"
	StatusDevolucao    OrderStatus = "Devolução"
	StatusNaoCortado   OrderStatus = "Não cortado"
	StatusCortado      OrderStatus = "Cortado"
	StatusNaoConferido OrderStatus = "Não conferido"
	StatusConferido    OrderStatus = "Conferido"
)

type OrderStage string

const (
	StageDesign      OrderStage = "Design"
	StageImpressao   OrderStage = "Impressão"
	StageSublimacao  OrderStage = "Sublimação"
	StageCostura     OrderStage = "Costura"
	StageExpedicao   OrderStage = "Expedição"
	StageCorte       OrderStage = "Corte"
	StageConferencia OrderStage = "Conferência"
)

type OrderType string

const (
	TypePrazoNormal  OrderType = "Prazo normal"
	TypeAntecipacao  OrderType = "Antecipação"
	TypeFaturado     OrderType = "Faturado"
	TypeMetadeMetade OrderType = "Metade/Metade"
	TypeAmostra      OrderType = "Amostra"
	TypeReposicao    OrderType = "Reposição"
)

type TinyOrder struct {
	ID     string `json:"id,omitempty" bson:"id,omitempty"`
	Number string `json:"number,omitempty" bson:"number,omitempty"`
}

type MongoDBOrders struct {
	ID                 bson.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	OldID              uint64        `json:"old_id" bson:"old_id"`
	CreatedBy          bson.ObjectID `json:"created_by,omitempty" bson:"created_by,omitempty"`
	RelatedSeller      bson.ObjectID `json:"related_seller,omitempty" bson:"related_seller,omitempty"`
	RelatedDesigner    bson.ObjectID `json:"related_designer,omitempty" bson:"related_designer,omitempty"`
	TrackingCode       string        `json:"tracking_code,omitempty" bson:"tracking_code,omitempty"`
	Status             OrderStatus   `json:"status,omitempty" bson:"status,omitempty"`
	Stage              OrderStage    `json:"stage,omitempty" bson:"stage,omitempty"`
	Type               OrderType     `json:"type,omitempty" bson:"type,omitempty"`
	UrlTrello          string        `json:"url_trello,omitempty" bson:"url_trello,omitempty"`
	ProductsListLegacy string        `json:"products_list_legacy,omitempty" bson:"products_list_legacy,omitempty"`
	RelatedBudget      bson.ObjectID `json:"related_budget,omitempty" bson:"related_budget,omitempty"`
	ExpectedDate       time.Time     `json:"expected_date,omitempty" bson:"expected_date,omitempty"`
	Notes              string        `json:"notes,omitempty" bson:"notes,omitempty"`
	CreatedAt          time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at" bson:"updated_at"`
}

type MySQLOrders struct {
	ID                 uint64         `db:"id"`
	UserID             uint64         `db:"user_id"`
	NumeroPedido       sql.NullString `db:"numero_pedido"`
	PrazoArteFinal     sql.NullTime   `db:"prazo_arte_final"`
	PrazoConfeccao     sql.NullTime   `db:"prazo_confeccao"`
	ListaProdutos      sql.NullString `db:"lista_produtos"`
	Observacoes        sql.NullString `db:"observacoes"`
	Rolo               sql.NullString `db:"rolo"`
	PedidoStatusID     uint64         `db:"pedido_status_id"`
	PedidoTipoID       uint64         `db:"pedido_tipo_id"`
	Estagio            sql.NullString `db:"estagio"`
	UrlTrello          sql.NullString `db:"url_trello"`
	Situacao           sql.NullString `db:"situacao"`
	Prioridade         sql.NullString `db:"prioridade"`
	OrcamentoID        uint64         `db:"orcamento_id"`
	CreatedAt          sql.NullTime   `db:"created_at"`
	UpdatedAt          sql.NullTime   `db:"updated_at"`
	TinyPedidoID       sql.NullString `db:"tiny_pedido_id"`
	DataPrevista       sql.NullTime   `db:"data_prevista"`
	VendedorID         uint64         `db:"vendedor_id"`
	DesignerID         uint64         `db:"designer_id"`
	CodigoRastreamento sql.NullString `db:"codigo_rastreamento"`
}

func SyncOrders() error {
	mysqlURI := os.Getenv("MYSQL_URI")

	mysqlDB, err := sql.Open("mysql", mysqlURI)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer mysqlDB.Close()

	mysqlDB.SetConnMaxLifetime(database.MYSQL_CONN_MAX_LIFETIME)
	mysqlDB.SetMaxOpenConns(database.MYSQL_MAX_OPEN_CONNS)
	mysqlDB.SetMaxIdleConns(database.MYSQL_MAX_IDLE_CONNS)

	if err := mysqlDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), database.MONGODB_TIMEOUT)
	defer cancel()

	mongoURI := os.Getenv(utils.MONGODB_URI)
	opts := options.Client().ApplyURI(mongoURI)
	mongoClient, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer mongoClient.Disconnect(ctx)

	ordersCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_ORDERS)

	allOrdersMap := make(map[uint64]*MySQLOrders)

	dataRows, err := mysqlDB.Query("SELECT id FROM pedidos_arte_final WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL orders data: %w", err)
	}

	for dataRows.Next() {
		var id sql.NullInt64
		if err := dataRows.Scan(&id); err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL order data: %w", err)
		}

		if !id.Valid || id.Int64 <= 0 {
			continue
		}

		order := &MySQLOrders{
			ID: uint64(id.Int64),
		}
		allOrdersMap[uint64(id.Int64)] = order
	}
	dataRows.Close()

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	if len(allOrdersMap) == 0 {
		return nil
	}

	mysqlIDs := make(map[uint64]bool, len(allOrdersMap))
	for id := range allOrdersMap {
		mysqlIDs[id] = true
	}

	mongoIDs := make(map[uint64]bool)
	mongoOrdersData := make(map[uint64]MongoDBOrders)

	cursor, err := ordersCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB orders: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var order MongoDBOrders
		if err := cursor.Decode(&order); err != nil {
			return fmt.Errorf("failed to decode MongoDB order: %w", err)
		}
		if order.OldID > 0 {
			mongoIDs[order.OldID] = true
			mongoOrdersData[order.OldID] = order
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("error iterating MongoDB cursor: %w", err)
	}

	idsToDelete := []uint64{}
	for mongoID := range mongoIDs {
		if !mysqlIDs[mongoID] {
			idsToDelete = append(idsToDelete, mongoID)
		}
	}

	if len(idsToDelete) > 0 {
		deleteFilter := bson.D{{Key: "old_id", Value: bson.D{{Key: "$in", Value: idsToDelete}}}}
		_, err := ordersCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing orders from MongoDB: %w", err)
		}
	}

	idsToUpsert := []uint64{}
	for id := range allOrdersMap {
		_, exists := mongoOrdersData[id]
		if !exists {
			idsToUpsert = append(idsToUpsert, id)
		}
	}

	if len(idsToUpsert) == 0 {
		return nil
	}

	batchSize := 50
	if len(idsToUpsert) > 1000 {
		batchSize = 200
	} else if len(idsToUpsert) > 5000 {
		batchSize = 500
	}

	bulkOperations := []mongo.WriteModel{}

	for _, id := range idsToUpsert {
		mongoOrder := MongoDBOrders{
			OldID: id,
		}

		filter := bson.D{{Key: "old_id", Value: mongoOrder.OldID}}
		update := bson.D{{Key: "$set", Value: mongoOrder}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)

		if len(bulkOperations) >= batchSize {
			if _, err := ordersCollection.BulkWrite(ctx, bulkOperations); err != nil {
				return fmt.Errorf("failed to execute bulk write: %w", err)
			}
			bulkOperations = []mongo.WriteModel{}
		}
	}

	if len(bulkOperations) > 0 {
		if _, err := ordersCollection.BulkWrite(ctx, bulkOperations); err != nil {
			return fmt.Errorf("failed to execute final bulk write: %w", err)
		}
	}

	return nil
}

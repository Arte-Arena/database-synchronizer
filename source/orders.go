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
	CustomProperties   any           `json:"custom_properties,omitempty" bson:"custom_properties,omitempty"`
	Tiny               TinyOrder     `json:"tiny,omitempty" bson:"tiny,omitempty"`
	Notes              string        `json:"notes,omitempty" bson:"notes,omitempty"`
	CreatedAt          time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at" bson:"updated_at"`
}

type MySQLOrders struct {
	ID                 sql.NullInt64  `db:"id"`
	UserID             sql.NullInt64  `db:"user_id"`
	NumeroPedido       sql.NullString `db:"numero_pedido"`
	PrazoArteFinal     sql.NullString `db:"prazo_arte_final"`
	PrazoConfeccao     sql.NullString `db:"prazo_confeccao"`
	ListaProdutos      sql.NullString `db:"lista_produtos"`
	Observacoes        sql.NullString `db:"observacoes"`
	Rolo               sql.NullString `db:"rolo"`
	PedidoStatusID     sql.NullInt64  `db:"pedido_status_id"`
	PedidoTipoID       sql.NullInt64  `db:"pedido_tipo_id"`
	Estagio            sql.NullString `db:"estagio"`
	UrlTrello          sql.NullString `db:"url_trello"`
	Situacao           sql.NullString `db:"situacao"`
	Prioridade         sql.NullString `db:"prioridade"`
	OrcamentoID        sql.NullInt64  `db:"orcamento_id"`
	CreatedAt          sql.NullString `db:"created_at"`
	UpdatedAt          sql.NullString `db:"updated_at"`
	TinyPedidoID       sql.NullString `db:"tiny_pedido_id"`
	DataPrevista       sql.NullString `db:"data_prevista"`
	VendedorID         sql.NullInt64  `db:"vendedor_id"`
	DesignerID         sql.NullInt64  `db:"designer_id"`
	CodigoRastreamento sql.NullString `db:"codigo_rastreamento"`
}

var statusIDToOrderStatus = map[uint64]OrderStatus{
	1:  StatusPendente,
	2:  StatusEmAndamento,
	3:  StatusArteOK,
	4:  StatusEmEspera,
	5:  StatusCorTeste,
	8:  StatusPendente,
	9:  StatusProcessando,
	10: StatusImpresso,
	12: StatusEmImpressao,
	13: StatusSeparacao,
	14: StatusCosturado,
	15: StatusPendente,
	20: StatusPrensa,
	21: StatusCalandra,
	22: StatusEmSeparacao,
	23: StatusRetirada,
	24: StatusEmEntrega,
	25: StatusEntregue,
	26: StatusDevolucao,
	27: StatusNaoCortado,
	28: StatusCortado,
	29: StatusNaoConferido,
	30: StatusConferido,
	31: StatusPendente,
	32: StatusPendente,
}

var tipoIDToOrderType = map[uint64]OrderType{
	1: TypePrazoNormal,
	2: TypeAntecipacao,
	3: TypeFaturado,
	4: TypeMetadeMetade,
	5: TypeAmostra,
	6: TypeReposicao,
}

var letraToOrderStage = map[string]OrderStage{
	"D": StageDesign,
	"I": StageImpressao,
	"S": StageSublimacao,
	"C": StageCostura,
	"E": StageExpedicao,
	"R": StageCorte,
	"F": StageConferencia,
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
	usersCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_USERS)
	budgetsCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_BUDGETS)

	userOldIDToObjectID := make(map[uint64]bson.ObjectID)
	userCursor, err := usersCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB users: %w", err)
	}
	for userCursor.Next(ctx) {
		var user struct {
			ID    bson.ObjectID `bson:"_id"`
			OldID uint64        `bson:"old_id"`
		}
		if err := userCursor.Decode(&user); err == nil && user.OldID > 0 {
			userOldIDToObjectID[user.OldID] = user.ID
		}
	}
	userCursor.Close(ctx)

	budgetOldIDToObjectID := make(map[uint64]bson.ObjectID)
	budgetCursor, err := budgetsCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB budgets: %w", err)
	}
	for budgetCursor.Next(ctx) {
		var budget struct {
			ID    bson.ObjectID `bson:"_id"`
			OldID uint64        `bson:"old_id"`
		}
		if err := budgetCursor.Decode(&budget); err == nil && budget.OldID > 0 {
			budgetOldIDToObjectID[budget.OldID] = budget.ID
		}
	}
	budgetCursor.Close(ctx)

	allOrdersMap := make(map[uint64]*MySQLOrders)

	dataRows, err := mysqlDB.Query(`SELECT id, user_id, numero_pedido, prazo_arte_final, prazo_confeccao, lista_produtos, observacoes, rolo, pedido_status_id, pedido_tipo_id, estagio, url_trello, situacao, prioridade, orcamento_id, created_at, updated_at, tiny_pedido_id, data_prevista, vendedor_id, designer_id, codigo_rastreamento FROM pedidos_arte_final WHERE id IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("failed to query MySQL orders data: %w", err)
	}

	for dataRows.Next() {
		order := &MySQLOrders{}
		err := dataRows.Scan(
			&order.ID,
			&order.UserID,
			&order.NumeroPedido,
			&order.PrazoArteFinal,
			&order.PrazoConfeccao,
			&order.ListaProdutos,
			&order.Observacoes,
			&order.Rolo,
			&order.PedidoStatusID,
			&order.PedidoTipoID,
			&order.Estagio,
			&order.UrlTrello,
			&order.Situacao,
			&order.Prioridade,
			&order.OrcamentoID,
			&order.CreatedAt,
			&order.UpdatedAt,
			&order.TinyPedidoID,
			&order.DataPrevista,
			&order.VendedorID,
			&order.DesignerID,
			&order.CodigoRastreamento,
		)
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL order data: %w", err)
		}
		if order.ID.Valid {
			allOrdersMap[uint64(order.ID.Int64)] = order
		}
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
		idsToUpsert = append(idsToUpsert, id)
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
		order := allOrdersMap[id]
		mongoOrder := bson.D{{Key: "old_id", Value: id}}

		if order.UserID.Valid {
			if oid, ok := userOldIDToObjectID[uint64(order.UserID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "created_by", Value: oid})
			}
		}

		if order.VendedorID.Valid {
			if oid, ok := userOldIDToObjectID[uint64(order.VendedorID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "related_seller", Value: oid})
			}
		}

		if order.DesignerID.Valid {
			if oid, ok := userOldIDToObjectID[uint64(order.DesignerID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "related_designer", Value: oid})
			}
		}

		if order.OrcamentoID.Valid {
			if oid, ok := budgetOldIDToObjectID[uint64(order.OrcamentoID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "related_budget", Value: oid})
			}
		}

		if order.CodigoRastreamento.Valid {
			mongoOrder = append(mongoOrder, bson.E{Key: "tracking_code", Value: order.CodigoRastreamento.String})
		}

		if order.PedidoStatusID.Valid {
			if status, ok := statusIDToOrderStatus[uint64(order.PedidoStatusID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "status", Value: status})
			}
		}

		if order.Estagio.Valid {
			if stage, ok := letraToOrderStage[order.Estagio.String]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "stage", Value: stage})
			}
		}

		if order.PedidoTipoID.Valid {
			if orderType, ok := tipoIDToOrderType[uint64(order.PedidoTipoID.Int64)]; ok {
				mongoOrder = append(mongoOrder, bson.E{Key: "type", Value: orderType})
			}
		}

		if order.UrlTrello.Valid {
			mongoOrder = append(mongoOrder, bson.E{Key: "url_trello", Value: order.UrlTrello.String})
		}

		if order.ListaProdutos.Valid {
			mongoOrder = append(mongoOrder, bson.E{Key: "products_list_legacy", Value: order.ListaProdutos.String})
		}

		if order.PrazoArteFinal.Valid {
			t, err := time.Parse("2006-01-02 15:04:05", order.PrazoArteFinal.String)
			if err == nil {
				mongoOrder = append(mongoOrder, bson.E{Key: "prazo_arte_final", Value: t})
			}
		}

		if order.PrazoConfeccao.Valid {
			t, err := time.Parse("2006-01-02 15:04:05", order.PrazoConfeccao.String)
			if err == nil {
				mongoOrder = append(mongoOrder, bson.E{Key: "prazo_confeccao", Value: t})
			}
		}

		if order.DataPrevista.Valid {
			t, err := time.Parse("2006-01-02 15:04:05", order.DataPrevista.String)
			if err == nil {
				mongoOrder = append(mongoOrder, bson.E{Key: "expected_date", Value: t})
			}
		}

		if order.Observacoes.Valid {
			mongoOrder = append(mongoOrder, bson.E{Key: "notes", Value: order.Observacoes.String})
		}

		if order.TinyPedidoID.Valid {
			tinyField := bson.M{}
			if order.TinyPedidoID.Valid {
				tinyField["id"] = order.TinyPedidoID.String
			}
			if order.NumeroPedido.Valid {
				tinyField["number"] = order.NumeroPedido.String
			}
			if len(tinyField) > 0 {
				mongoOrder = append(mongoOrder, bson.E{Key: "tiny", Value: tinyField})
			}
		}

		if order.CreatedAt.Valid {
			t, err := time.Parse("2006-01-02 15:04:05", order.CreatedAt.String)
			if err == nil {
				mongoOrder = append(mongoOrder, bson.E{Key: "created_at", Value: t})
			}
		}

		if order.UpdatedAt.Valid {
			t, err := time.Parse("2006-01-02 15:04:05", order.UpdatedAt.String)
			if err == nil {
				mongoOrder = append(mongoOrder, bson.E{Key: "updated_at", Value: t})
			}
		}

		filter := bson.D{{Key: "old_id", Value: id}}
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

package main

import (
	"context"
	"database/sql"
	"database_sync/database"
	"database_sync/utils"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoDBDelivery struct {
	Option   string  `json:"option" bson:"option"`
	Deadline uint    `json:"deadline" bson:"deadline"`
	Price    float64 `json:"price" bson:"price"`
}

type MongoDBEarlyMode struct {
	Date time.Time `json:"date" bson:"date"`
	Tax  float64   `json:"tax" bson:"tax"`
}

type MongoDBDiscount struct {
	Type       string  `json:"type" bson:"type"`
	Value      float64 `json:"value" bson:"value"`
	Percentage float64 `json:"percentage" bson:"percentage"`
}

type MongoDBInstallments struct {
	Date  time.Time `json:"date" bson:"date"`
	Value float64   `json:"value" bson:"value"`
}

type MongoDBBilling struct {
	Type         string                `json:"type" bson:"type"`
	Installments []MongoDBInstallments `json:"installments" bson:"installments"`
}

type MongoDBAddress struct {
	CEP     string `json:"cep" bson:"cep"`
	Details string `json:"details" bson:"details"`
}

type MongoDBBudgets struct {
	ID                 bson.ObjectID    `json:"id,omitempty" bson:"_id,omitempty"`
	OldID              uint64           `json:"old_id" bson:"old_id"`
	CreatedBy          bson.ObjectID    `json:"created_by" bson:"created_by"`
	Seller             bson.ObjectID    `json:"seller" bson:"seller"`
	RelatedLead        bson.ObjectID    `json:"related_lead" bson:"related_lead"`
	RelatedClient      bson.ObjectID    `json:"related_client" bson:"related_client"`
	OldProductsList    string           `json:"old_products_list" bson:"old_products_list"`
	Address            MongoDBAddress   `json:"address" bson:"address"`
	Delivery           MongoDBDelivery  `json:"delivery" bson:"delivery"`
	EarlyMode          MongoDBEarlyMode `json:"early_mode" bson:"early_mode"`
	Discount           MongoDBDiscount  `json:"discount" bson:"discount"`
	OldGifts           string           `json:"old_gifts" bson:"old_gifts"`
	ProductionDeadline uint             `json:"production_deadline" bson:"production_deadline"`
	Approved           bool             `json:"approved" bson:"approved"`
	PaymentMethod      string           `json:"payment_method" bson:"payment_method"`
	Billing            MongoDBBilling   `json:"billing" bson:"billing"`
	Trello_uri         string           `json:"trello_uri" bson:"trello_uri"`
	Notes              string           `json:"notes" bson:"notes"`
	DeliveryForecast   time.Time        `json:"delivery_forecast" bson:"delivery_forecast"`
	CreatedAt          time.Time        `json:"created_at" bson:"created_at,omitempty"`
	UpdatedAt          time.Time        `json:"updated_at" bson:"updated_at,omitempty"`
}

type MySQLBudgets struct {
	ID                 uint64          `db:"id"`
	UserID             sql.NullInt64   `db:"user_id"`
	ClienteOctaNumber  sql.NullString  `db:"cliente_octa_number"`
	NomeCliente        sql.NullString  `db:"nome_cliente"`
	ListaProdutos      sql.NullString  `db:"lista_produtos"`
	TextoOrcamento     sql.NullString  `db:"texto_orcamento"`
	EnderecoCep        sql.NullString  `db:"endereco_cep"`
	Endereco           sql.NullString  `db:"endereco"`
	OpcaoEntrega       sql.NullString  `db:"opcao_entrega"`
	PrazoOpcaoEntrega  sql.NullInt64   `db:"prazo_opcao_entrega"`
	PrecoOpcaoEntrega  sql.NullFloat64 `db:"preco_opcao_entrega"`
	CreatedAt          time.Time       `db:"created_at"`
	UpdatedAt          sql.NullTime    `db:"updated_at"`
	Antecipado         sql.NullInt64   `db:"antecipado"`
	DataAntecipacao    sql.NullTime    `db:"data_antecipa"`
	TaxaAntecipacao    sql.NullFloat64 `db:"taxa_antecipa"`
	Descontado         sql.NullInt64   `db:"descontado"`
	TipoDesconto       sql.NullString  `db:"tipo_desconto"`
	ValorDesconto      sql.NullFloat64 `db:"valor_desconto"`
	PercentualDesconto sql.NullFloat64 `db:"percentual_desconto"`
	TotalOrcamento     sql.NullFloat64 `db:"total_orcamento"`
	Brinde             sql.NullInt64   `db:"brinde"`
	ProdutosBrinde     sql.NullString  `db:"produtos_brinde"`
	PrazoProducao      sql.NullInt64   `db:"prazo_producao"`
	PrevEntrega        sql.NullTime    `db:"prev_entrega"`
}

type MySQLBudgetsStatus struct {
	ID                uint64          `db:"id"`
	UserID            sql.NullInt64   `db:"user_id"`
	OrcamentoID       uint64          `db:"orcamento_id"`
	Status            sql.NullString  `db:"status"`
	FormaPagamento    sql.NullString  `db:"forma_pagamento"`
	TipoFaturamento   sql.NullString  `db:"tipo_faturamento"`
	DataFaturamento   sql.NullTime    `db:"data_faturamento"`
	QtdParcelas       sql.NullInt64   `db:"qtd_parcelas"`
	LinkTrello        sql.NullString  `db:"link_trello"`
	Comentarios       sql.NullString  `db:"comentarios"`
	DataFaturamento2  sql.NullTime    `db:"data_faturamento_2"`
	DataFaturamento3  sql.NullTime    `db:"data_faturamento_3"`
	ValorFaturamento  sql.NullFloat64 `db:"valor_faturamento"`
	ValorFaturamento2 sql.NullFloat64 `db:"valor_faturamento_2"`
	ValorFaturamento3 sql.NullFloat64 `db:"valor_faturamento_3"`
}

func SyncBudgets() error {
	mysqlURI := os.Getenv("MYSQL_URI")
	if !strings.Contains(mysqlURI, "parseTime=true") {
		if strings.Contains(mysqlURI, "?") {
			mysqlURI += "&parseTime=true"
		} else {
			mysqlURI += "?parseTime=true"
		}
	}

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

	usersCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_USERS)
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

	allBudgetsMap := make(map[uint64]*MySQLBudgets)

	dataRows, err := mysqlDB.Query("SELECT id, user_id, cliente_octa_number, nome_cliente, lista_produtos, texto_orcamento, endereco_cep, endereco, opcao_entrega, prazo_opcao_entrega, preco_opcao_entrega, created_at, updated_at, antecipado, data_antecipa, taxa_antecipa, descontado, tipo_desconto, valor_desconto, percentual_desconto, total_orcamento, brinde, produtos_brinde, prazo_producao, prev_entrega FROM orcamentos WHERE id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to query MySQL orcamentos data: %w", err)
	}
	defer dataRows.Close()

	for dataRows.Next() {
		budget := &MySQLBudgets{}

		err := dataRows.Scan(
			&budget.ID, &budget.UserID, &budget.ClienteOctaNumber, &budget.NomeCliente,
			&budget.ListaProdutos, &budget.TextoOrcamento, &budget.EnderecoCep,
			&budget.Endereco, &budget.OpcaoEntrega, &budget.PrazoOpcaoEntrega,
			&budget.PrecoOpcaoEntrega, &budget.CreatedAt, &budget.UpdatedAt,
			&budget.Antecipado, &budget.DataAntecipacao, &budget.TaxaAntecipacao,
			&budget.Descontado, &budget.TipoDesconto, &budget.ValorDesconto,
			&budget.PercentualDesconto, &budget.TotalOrcamento, &budget.Brinde,
			&budget.ProdutosBrinde, &budget.PrazoProducao, &budget.PrevEntrega,
		)
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to scan MySQL budget data: %w", err)
		}

		allBudgetsMap[budget.ID] = budget
	}

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL data rows: %w", err)
	}

	allBudgetStatusesMap := make(map[uint64]*MySQLBudgetsStatus)
	statusRows, err := mysqlDB.Query("SELECT id, user_id, orcamento_id, status, forma_pagamento, tipo_faturamento, data_faturamento, qtd_parcelas, link_trello, comentarios, data_faturamento_2, data_faturamento_3, valor_faturamento, valor_faturamento_2, valor_faturamento_3 FROM orcamentos_status")
	if err != nil {
		return fmt.Errorf("failed to query MySQL orcamentos_status data: %w", err)
	}
	defer statusRows.Close()

	for statusRows.Next() {
		status := &MySQLBudgetsStatus{}
		err := statusRows.Scan(
			&status.ID, &status.UserID, &status.OrcamentoID, &status.Status, &status.FormaPagamento,
			&status.TipoFaturamento, &status.DataFaturamento, &status.QtdParcelas, &status.LinkTrello,
			&status.Comentarios, &status.DataFaturamento2, &status.DataFaturamento3,
			&status.ValorFaturamento, &status.ValorFaturamento2, &status.ValorFaturamento3,
		)
		if err != nil {
			statusRows.Close()
			return fmt.Errorf("failed to scan MySQL orcamentos_status data: %w", err)
		}
		allBudgetStatusesMap[status.OrcamentoID] = status
	}

	if err = statusRows.Err(); err != nil {
		return fmt.Errorf("error iterating MySQL orcamentos_status rows: %w", err)
	}

	if len(allBudgetsMap) == 0 {
		fmt.Printf("[SYNC_BUDGETS] No records found in MySQL to synchronize: %s\n",
			time.Now().Format("2006-01-02 15:04:05"))
		return nil
	}

	mysqlIDs := make(map[uint64]bool, len(allBudgetsMap))
	for id := range allBudgetsMap {
		mysqlIDs[id] = true
	}

	budgetsCollection := mongoClient.Database(database.GetDB()).Collection(database.COLLECTION_BUDGETS)
	mongoIDs := make(map[uint64]bool)
	mongoBudgetsData := make(map[uint64]MongoDBBudgets)

	cursor, err := budgetsCollection.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to query MongoDB budgets: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var budget MongoDBBudgets
		if err := cursor.Decode(&budget); err != nil {
			return fmt.Errorf("failed to decode MongoDB budget: %w", err)
		}
		if budget.OldID > 0 {
			mongoIDs[budget.OldID] = true
			mongoBudgetsData[budget.OldID] = budget
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
		_, err := budgetsCollection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			return fmt.Errorf("failed to delete non-existing budgets from MongoDB: %w", err)
		}
	}

	recordsToUpsert := make([]uint64, 0)
	for id := range allBudgetsMap {
		recordsToUpsert = append(recordsToUpsert, id)
	}

	if len(recordsToUpsert) == 0 {
		return nil
	}

	totalRecords := len(recordsToUpsert)
	batchSize := 50
	if totalRecords > 1000 {
		batchSize = 200
	} else if totalRecords > 5000 {
		batchSize = 500
	}

	bulkOperations := []mongo.WriteModel{}
	processedCount := 0
	bulkWriteCount := 0

	for _, id := range recordsToUpsert {
		budget := allBudgetsMap[id]
		budgetStatus, hasStatus := allBudgetStatusesMap[id]

		mongoBudget := bson.D{
			{Key: "old_id", Value: id},
			{Key: "created_at", Value: budget.CreatedAt},
		}

		if budget.UserID.Valid {
			if oid, ok := userOldIDToObjectID[uint64(budget.UserID.Int64)]; ok {
				mongoBudget = append(mongoBudget, bson.E{Key: "created_by", Value: oid})
			}
		}

		if hasStatus && budgetStatus.UserID.Valid {
			if oid, ok := userOldIDToObjectID[uint64(budgetStatus.UserID.Int64)]; ok {
				mongoBudget = append(mongoBudget, bson.E{Key: "seller", Value: oid})
			}
		}

		if budget.ListaProdutos.Valid {
			mongoBudget = append(mongoBudget, bson.E{Key: "old_products_list", Value: budget.ListaProdutos.String})
		}

		address := MongoDBAddress{}
		if budget.EnderecoCep.Valid {
			address.CEP = budget.EnderecoCep.String
		}
		if budget.Endereco.Valid {
			address.Details = budget.Endereco.String
		}
		mongoBudget = append(mongoBudget, bson.E{Key: "address", Value: address})

		delivery := MongoDBDelivery{}
		if budget.OpcaoEntrega.Valid {
			delivery.Option = budget.OpcaoEntrega.String
		}
		if budget.PrazoOpcaoEntrega.Valid {
			delivery.Deadline = uint(budget.PrazoOpcaoEntrega.Int64)
		}
		if budget.PrecoOpcaoEntrega.Valid {
			delivery.Price = budget.PrecoOpcaoEntrega.Float64
		}
		mongoBudget = append(mongoBudget, bson.E{Key: "delivery", Value: delivery})

		if budget.Antecipado.Valid && budget.Antecipado.Int64 == 1 {
			earlyMode := MongoDBEarlyMode{}
			if budget.DataAntecipacao.Valid {
				earlyMode.Date = budget.DataAntecipacao.Time
			}
			if budget.TaxaAntecipacao.Valid {
				earlyMode.Tax = budget.TaxaAntecipacao.Float64
			}
			mongoBudget = append(mongoBudget, bson.E{Key: "early_mode", Value: earlyMode})
		}

		if budget.Descontado.Valid && budget.Descontado.Int64 == 1 {
			discount := MongoDBDiscount{}
			if budget.TipoDesconto.Valid {
				discount.Type = budget.TipoDesconto.String
			}
			if budget.ValorDesconto.Valid {
				discount.Value = budget.ValorDesconto.Float64
			}
			if budget.PercentualDesconto.Valid {
				discount.Percentage = budget.PercentualDesconto.Float64
			}
			mongoBudget = append(mongoBudget, bson.E{Key: "discount", Value: discount})
		}

		if budget.Brinde.Valid && budget.Brinde.Int64 == 1 && budget.ProdutosBrinde.Valid {
			mongoBudget = append(mongoBudget, bson.E{Key: "old_gifts", Value: budget.ProdutosBrinde.String})
		}

		if budget.PrazoProducao.Valid {
			mongoBudget = append(mongoBudget, bson.E{Key: "production_deadline", Value: uint(budget.PrazoProducao.Int64)})
		}

		if budget.PrevEntrega.Valid {
			mongoBudget = append(mongoBudget, bson.E{Key: "delivery_forecast", Value: budget.PrevEntrega.Time})
		}

		if budget.UpdatedAt.Valid {
			mongoBudget = append(mongoBudget, bson.E{Key: "updated_at", Value: budget.UpdatedAt.Time})
		}

		if hasStatus {
			approved := false
			if budgetStatus.Status.Valid && strings.EqualFold(budgetStatus.Status.String, "aprovado") {
				approved = true
			}
			mongoBudget = append(mongoBudget, bson.E{Key: "approved", Value: approved})
			if budgetStatus.FormaPagamento.Valid {
				mongoBudget = append(mongoBudget, bson.E{Key: "payment_method", Value: budgetStatus.FormaPagamento.String})
			}

			billing := MongoDBBilling{}
			if budgetStatus.TipoFaturamento.Valid {
				billing.Type = budgetStatus.TipoFaturamento.String
			}

			var installments []MongoDBInstallments
			if budgetStatus.DataFaturamento.Valid && budgetStatus.ValorFaturamento.Valid {
				installments = append(installments, MongoDBInstallments{Date: budgetStatus.DataFaturamento.Time, Value: budgetStatus.ValorFaturamento.Float64})
			}
			if budgetStatus.DataFaturamento2.Valid && budgetStatus.ValorFaturamento2.Valid {
				installments = append(installments, MongoDBInstallments{Date: budgetStatus.DataFaturamento2.Time, Value: budgetStatus.ValorFaturamento2.Float64})
			}
			if budgetStatus.DataFaturamento3.Valid && budgetStatus.ValorFaturamento3.Valid {
				installments = append(installments, MongoDBInstallments{Date: budgetStatus.DataFaturamento3.Time, Value: budgetStatus.ValorFaturamento3.Float64})
			}
			billing.Installments = installments
			mongoBudget = append(mongoBudget, bson.E{Key: "billing", Value: billing})

			if budgetStatus.LinkTrello.Valid {
				mongoBudget = append(mongoBudget, bson.E{Key: "trello_uri", Value: budgetStatus.LinkTrello.String})
			}
			if budgetStatus.Comentarios.Valid {
				mongoBudget = append(mongoBudget, bson.E{Key: "notes", Value: budgetStatus.Comentarios.String})
			}
		} else {
			mongoBudget = append(mongoBudget, bson.E{Key: "approved", Value: false})
		}

		filter := bson.D{{Key: "old_id", Value: id}}
		update := bson.D{{Key: "$set", Value: mongoBudget}}

		upsertModel := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		bulkOperations = append(bulkOperations, upsertModel)
		processedCount++

		if len(bulkOperations) >= batchSize {
			_, err := budgetsCollection.BulkWrite(ctx, bulkOperations)
			if err != nil {
				return fmt.Errorf("failed to bulk write to MongoDB: %w", err)
			}

			bulkOperations = []mongo.WriteModel{}
			bulkWriteCount++
		}
	}

	if len(bulkOperations) > 0 {
		_, err := budgetsCollection.BulkWrite(ctx, bulkOperations)
		if err != nil {
			return fmt.Errorf("failed to bulk write remaining documents to MongoDB: %w", err)
		}
		bulkWriteCount++
	}

	return nil
}

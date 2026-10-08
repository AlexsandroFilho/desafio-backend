package main

import (
	"alexsandro-filho/desafio-backend/internal/app"
	"go.uber.org/fx"
)

func main() {
	fx.New(app.Module).Run()
}

package main

import (
	"fmt"
	"github.com/jawher/mow.cli"
	"github.com/tealeg/xlsx"
	"log"
	"os"
)

type mapping struct {
	compositeID string
	fsId        string
}

func main() {
	app := cli.App("composite-organisations-transformer", "A RESTful API for transforming combined organisations")
	concordanceFile := app.String(cli.StringOpt{
		Name:  "concordance-xlsx",
		Value: "",
		Desc:  "Filename for concordance xlsx",
	})

	app.Action = func() {
		if *concordanceFile == "" {
			log.Fatal("concordance file must be provided")
		}
		xlFile, err := xlsx.OpenFile(*concordanceFile)
		if err != nil {
			log.Fatal(err)
		}

		fromComposite := make(map[string]mapping)
		fromFs := make(map[string]mapping)

		for _, sheet := range xlFile.Sheets {
			for _, row := range sheet.Rows {

				compositeID, err := row.Cells[0].String()
				if err != nil {
					log.Fatal(err)
				}
				fsId, err := row.Cells[4].String()
				if err != nil {
					log.Fatal(err)
				}

				m := mapping{
					compositeID: compositeID,
					fsId:        fsId,
				}

				fromComposite[m.compositeID] = m
				fromFs[m.fsId] = m
			}
		}

		for _, x := range fromComposite {
			fmt.Printf("%v %v\n", x.compositeID, x.fsId)
		}
	}

	app.Run(os.Args)
}

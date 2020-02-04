package storage

func (m *Miner) fillData() {

	for range m.dataTiker.C {
		//nofill := os.Getenv("LOTUS_NOFILL")
		if m.sb.GetFreeWorkers() > 0 && !m.sb.Busy() {
			log.Info("[qz ] filling data")
			m.PledgeSector()
		}
	}
}

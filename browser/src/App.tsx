import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import DocumentsPage from './pages/DocumentsPage'
import FulltextSearchPage from './pages/FulltextSearchPage'
import VectorSearchPage from './pages/VectorSearchPage'

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          <Route path="/" element={<DocumentsPage />} />
          <Route path="/fulltext" element={<FulltextSearchPage />} />
          <Route path="/vector" element={<VectorSearchPage />} />
        </Routes>
      </Layout>
    </Router>
  )
}

export default App


package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/versions"
	"os"
)

const MaxImage = 128 << 20

// ErrImageTooLarge reports that an encoded image exceeded the MaxImage budget.
var ErrImageTooLarge = errors.New("image exceeds 128MiB limit")

type Image struct {
	Format   int              `json:"format"`
	Sequence uint64           `json:"sequence"`
	Horizon  uint64           `json:"horizon"`
	Catalog  *catalog.Catalog `json:"catalog"`
	Store    *versions.Store  `json:"store"`
}
type Envelope struct {
	Format  int             `json:"format"`
	Digest  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

func Encode(image Image) ([]byte, error) {
	image.Format = 1
	b, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return json.Marshal(Envelope{Format: 1, Digest: hex.EncodeToString(h[:]), Payload: b})
}

// trailingNewlineStripper removes the single '\n' json.Encoder appends after
// every Encode, even when it arrives in the same Write as the final bytes. It
// keeps the hashed stream and the persisted stream identical to json.Marshal.
type trailingNewlineStripper struct {
	w    io.Writer
	held byte
	has  bool
}

func (s *trailingNewlineStripper) Write(p []byte) (int, error) {
	if s.has {
		if _, err := s.w.Write([]byte{s.held}); err != nil {
			return 0, err
		}
		s.has = false
	}
	if len(p) == 0 {
		return 0, nil
	}
	s.held = p[len(p)-1]
	s.has = true
	if len(p) > 1 {
		if _, err := s.w.Write(p[:len(p)-1]); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (s *trailingNewlineStripper) Flush() error {
	if !s.has {
		return nil
	}
	if s.held != '\n' {
		if _, err := s.w.Write([]byte{s.held}); err != nil {
			return err
		}
	}
	s.has = false
	return nil
}

// EncodeTo streams one image envelope to w without materializing the full
// image in memory. The envelope byte layout is identical to Encode, so files
// produced here remain accepted by Decode and the restore path. Only the
// payload is hashed, matching Envelope semantics. The digest is back-patched
// into the header after the payload has been fully written, which requires w
// to support seeking (a regular spool file). Total size is bounded by
// MaxImage; ErrImageTooLarge is returned once the budget is crossed.
func EncodeTo(w io.WriteSeeker, image Image) (digest string, size int64, err error) {
	image.Format = 1
	hasher := sha256.New()
	if _, err = w.Write([]byte(`{"format":1,"sha256":"`)); err != nil {
		return "", 0, err
	}
	digestAt, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", 0, err
	}
	if _, err = w.Write(make([]byte, 64)); err != nil {
		return "", 0, err
	}
	if _, err = w.Write([]byte(`","payload":`)); err != nil {
		return "", 0, err
	}
	payloadAt, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", 0, err
	}
	// The stripper removes json.Encoder's trailing '\n' from both the
	// persisted bytes and the hashed payload, matching json.Marshal exactly.
	stripper := &trailingNewlineStripper{w: io.MultiWriter(w, hasher)}
	// Account for the fixed header and closing brace so the whole envelope
	// stays within MaxImage, not just the payload.
	overhead := int64(len(`{"format":1,"sha256":""`) + 64 + len(`,"payload":`) + 1)
	limited := &limitedWriter{w: stripper, remaining: MaxImage + 1 - overhead}
	encoder := json.NewEncoder(limited)
	if err = encoder.Encode(image); err != nil {
		if errors.Is(err, ErrImageTooLarge) {
			return "", 0, ErrImageTooLarge
		}
		return "", 0, err
	}
	if err = stripper.Flush(); err != nil {
		return "", 0, err
	}
	if limited.remaining <= 0 {
		return "", 0, ErrImageTooLarge
	}
	if _, err = w.Write([]byte(`}`)); err != nil {
		return "", 0, err
	}
	digest = hex.EncodeToString(hasher.Sum(nil))
	if _, err = w.Seek(digestAt, io.SeekStart); err != nil {
		return "", 0, err
	}
	if _, err = w.Write([]byte(digest)); err != nil {
		return "", 0, err
	}
	end, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		return "", 0, err
	}
	if end > MaxImage {
		return "", 0, ErrImageTooLarge
	}
	// Guard against an unexpected drift from Encode's field layout.
	if digestAt != int64(len(`{"format":1,"sha256":"`)) || payloadAt != digestAt+64+int64(len(`","payload":`)) {
		return "", 0, fmt.Errorf("envelope layout mismatch")
	}
	return digest, end, nil
}

type limitedWriter struct {
	w         io.Writer
	remaining int64
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, ErrImageTooLarge
	}
	if int64(len(p)) > l.remaining {
		return 0, ErrImageTooLarge
	}
	n, err := l.w.Write(p)
	l.remaining -= int64(n)
	return n, err
}
func Decode(b []byte) (Image, error) {
	var image Image
	var env Envelope
	if len(b) > MaxImage {
		return image, fmt.Errorf("image exceeds limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return image, err
	}
	h := sha256.Sum256(env.Payload)
	if env.Format != 1 || hex.EncodeToString(h[:]) != env.Digest {
		return image, fmt.Errorf("image format or digest mismatch")
	}
	if err := json.Unmarshal(env.Payload, &image); err != nil {
		return image, err
	}
	if err := Validate(image); err != nil {
		return image, err
	}
	return image, nil
}
func Read(path string) (Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, err
	}
	return Decode(b)
}
func Validate(image Image) error {
	if image.Format != 1 || image.Horizon > image.Sequence || image.Catalog == nil || image.Store == nil || image.Catalog.Relations == nil || image.Catalog.Indexes == nil || image.Store.Tables == nil {
		return fmt.Errorf("invalid image header")
	}
	for name, r := range image.Catalog.Relations {
		if name != r.Name || r.Created == 0 || r.Created > image.Sequence {
			return fmt.Errorf("invalid catalog sequence")
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	for name, definition := range image.Catalog.Indexes {
		relation, ok := image.Catalog.Relations[definition.Relation]
		if !ok || name != definition.Name || !catalog.ValidName(name) || definition.Created < relation.Created || definition.Created > image.Sequence {
			return fmt.Errorf("invalid index metadata")
		}
		if _, ok := relation.Column(definition.Column); !ok {
			return fmt.Errorf("invalid index column")
		}
	}
	for name, rows := range image.Store.Tables {
		r, ok := image.Catalog.Relations[name]
		if !ok {
			return fmt.Errorf("image references unknown relation")
		}
		for key, chain := range rows {
			var last uint64
			if len(chain) == 0 {
				return fmt.Errorf("empty version chain")
			}
			for _, v := range chain {
				if v.Sequence <= last || v.Sequence > image.Sequence || v.Sequence <= r.Created {
					return fmt.Errorf("invalid version order")
				}
				last = v.Sequence
				if !v.Deleted {
					row, err := r.Normalize(v.Row)
					if err != nil || r.Key(row) != key {
						return fmt.Errorf("invalid stored tuple")
					}
				}
			}
		}
	}
	for _, definition := range image.Catalog.Indexes {
		if !definition.Unique {
			continue
		}
		seen := map[string]bool{}
		for _, chain := range image.Store.Tables[definition.Relation] {
			version, exists := versions.Visible(chain, image.Sequence)
			if !exists {
				continue
			}
			value := version.Row[definition.Column]
			if value.Null() {
				continue
			}
			if seen[value.Key()] {
				return fmt.Errorf("image violates unique index %s", definition.Name)
			}
			seen[value.Key()] = true
		}
	}

	return nil
}
